# main.py
import argparse
import json
import logging
import re
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.window import FixedWindows

# ---------------- util: parse "30m", "1h", "2d" ----------------
_UNIT_TO_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
def parse_duration_to_seconds(text: str) -> int:
    m = re.fullmatch(r"(?i)\s*(\d+)\s*([smhd])\s*", text.strip())
    if not m:
        raise ValueError(f"window_duration inválido: {text!r} (use 30m, 1h, ...)")
    return int(m.group(1)) * _UNIT_TO_SECONDS[m.group(2).lower()]

# ---------------- helpers para Avro JSON ----------------
def _is_union(t):
    return isinstance(t, list)

def _non_null_types(union_list):
    return [x for x in union_list if not (isinstance(x, str) and x == "null")]

def _base_type(node):
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        return node.get("type")
    return None

# ---------------- Avro-JSON -> PyArrow ----------------
def _pyarrow_type_from_node(node, field_name=None):
    """
    Converte nó de tipo Avro-JSON para (pa.DataType, nullable).
    Regras mantidas:
      - logicalType timestamp-millis/micros -> pa.timestamp("ms"/"us")
      - unions com múltiplos tipos não-nulos -> string (conservador)
    """
    import pyarrow as pa

    # União
    if _is_union(node):
        non_null = _non_null_types(node)
        if not non_null:
            return pa.string(), True
        if len(non_null) > 1:
            return pa.string(), True
        t, n = _pyarrow_type_from_node(non_null[0], field_name=field_name)
        return t, True or n

    # Primitivos via string
    if isinstance(node, str):
        prim = node
        if prim == "string":  return pa.string(), False
        if prim == "boolean": return pa.bool_(), False
        if prim == "int":     return pa.int32(), False
        if prim == "long":    return pa.int64(), False
        if prim == "float":   return pa.float32(), False
        if prim == "double":  return pa.float64(), False
        if prim == "bytes":   return pa.binary(), False
        if prim == "null":    return pa.string(), True
        return pa.string(), True

    # Dict com "type"
    if isinstance(node, dict):
        t = node.get("type")
        logical = node.get("logicalType")

        # logicalType: timestamps
        if logical and t in {"long", "int"}:
            if logical == "timestamp-millis":
                return pa.timestamp("ms"), False
            if logical == "timestamp-micros":
                return pa.timestamp("us"), False
            # outros logicalTypes caem no tipo base

        if t == "string":  return pa.string(), False
        if t == "boolean": return pa.bool_(), False
        if t == "int":     return pa.int32(), False
        if t == "long":    return pa.int64(), False
        if t == "float":   return pa.float32(), False
        if t == "double":  return pa.float64(), False
        if t == "bytes":   return pa.binary(), False

        if t == "array":
            item_node = node.get("items")
            item_type, _ = _pyarrow_type_from_node(item_node, field_name=None)
            return pa.list_(item_type), True

        if t == "map":
            val_node = node.get("values")
            val_type, _ = _pyarrow_type_from_node(val_node)
            return pa.map_(pa.string(), val_type), True

        if t == "record":
            fields_json = node.get("fields", [])
            pa_fields = []
            for f in fields_json:
                fname = f["name"]
                ftype = f["type"]
                ptype, nullable = _pyarrow_type_from_node(ftype, field_name=fname)
                pa_fields.append(pa.field(fname, ptype, nullable=nullable))
            return pa.struct(pa_fields), False

        # fallback
        return pa.string(), True

    # fallback
    import pyarrow as pa  # local import
    return pa.string(), True

def _payload_struct_all_string_from_node(node):
    """Recebe nó Avro do campo 'payload' (record ou union contendo record) e devolve (pa.struct(strings...), nullable_payload)."""
    import pyarrow as pa
    nullable_payload = False
    record_node = None

    # Pode vir como union ["null", {"type":"record", ...}]
    if _is_union(node):
        non_null = _non_null_types(node)
        nullable_payload = len(non_null) < len(node)
        # pega o primeiro record não-nulo (se houver)
        for t in non_null:
            if isinstance(t, dict) and t.get("type") == "record":
                record_node = t
                break
    elif isinstance(node, dict) and node.get("type") == "record":
        record_node = node
    else:
        # Se não for record, força payload inteiro como string
        return pa.string(), True

    if not record_node:
        return pa.string(), True

    fields_json = record_node.get("fields", [])
    pa_fields = []
    for f in fields_json:
        fname = f["name"]
        # TODOS os campos internos como STRING (nullable, pois unions internos são comuns)
        pa_fields.append(pa.field(fname, pa.string(), nullable=True))
    return pa.struct(pa_fields), nullable_payload

def avro_schema_json_to_pyarrow_schema(avro_record_json):
    import pyarrow as pa
    if not isinstance(avro_record_json, dict) or avro_record_json.get("type") != "record":
        raise ValueError("Schema Avro inválido: esperado record com 'fields'.")

    pa_fields = []
    for f in avro_record_json.get("fields", []):
        name = f["name"]
        atype = f["type"]
        
        if name == "sort_keys":
            continue

        if name == "payload":
            # Força payload a ser STRUCT com todos os campos internos STRING
            ptype, nullable = _payload_struct_all_string_from_node(atype)
            pa_fields.append(pa.field(name, ptype, nullable=nullable))
            continue

        ptype, nullable = _pyarrow_type_from_node(atype, field_name=name)
        pa_fields.append(pa.field(name, ptype, nullable=nullable))

    return pa.schema(pa_fields)

# ---------------- Normalização de dados ----------------
def _stringify_any(x):
    """Converte qualquer valor para string; listas/dicts viram JSON estável."""
    if x is None:
        return None
    if isinstance(x, (list, dict, tuple)):
        try:
            return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(x)
    return str(x)

def _normalize_payload_values_to_string(rows):
    """Converte todos os valores internos de payload para string (mantendo chaves)."""
    for r in rows:
        r.pop("sort_keys", None)
        payload = r.get("payload")
        if isinstance(payload, dict):
            r["payload"] = {k: _stringify_any(v) for k, v in payload.items()}
        else:
            # Se payload não for dict (raro), transforma tudo numa string representando o conteúdo
            if payload is None:
                r["payload"] = None
            else:
                r["payload"] = _stringify_any(payload)

# ---------------- Beam DoFn: loga Avro & PyArrow; grava Parquet ----------------
class AvroPathLogPyArrowSchema(beam.DoFn):
    def __init__(self, output_prefix: str):
        self.output_prefix = output_prefix.rstrip("/")

    def process(self, path):
        import fastavro
        from uuid import uuid4
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Lê schema Avro e loga
        with FileSystems.open(path) as f:
            rdr = fastavro.reader(f)
            avro_schema = rdr.writer_schema

        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        schema_json = json.dumps(avro_schema, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        logging.info("[AVRO-SCHEMA] ingestion_date=%s schema=%s", ingestion_date, schema_json)
        logging.info("[AVRO-SCHEMA] example_file=%s", path)

        # Converte para PyArrow (payload.* => string)
        pa_schema = avro_schema_json_to_pyarrow_schema(avro_schema)
        logging.info("[PYARROW-SCHEMA] file=%s\n%s", path, pa_schema)

        # Reabre e lê registros Avro
        with FileSystems.open(path) as f2:
            rdr2 = fastavro.reader(f2)
            rows = [dict(r) for r in rdr2]

        # Normaliza payload para strings
        _normalize_payload_values_to_string(rows)

        #DEBUG: identifica campo com tipo Arrow incompatível com o valor Python
        for f in pa_schema:
            sample = next((r.get(f.name) for r in rows if r.get(f.name) is not None), None)
            if sample is None:
                continue
            import pyarrow as pa
            if pa.types.is_binary(f.type) and not isinstance(sample, (bytes, bytearray)):
                logging.warning("MISMATCH field=%s expected=%s got_py=%s value=%r", f.name, f.type, type(sample).__name__, sample)
            if pa.types.is_string(f.type) and not isinstance(sample, str):
                logging.warning("MISMATCH field=%s expected=%s got_py=%s value=%r", f.name, f.type, type(sample).__name__, sample)


        # Cria Tabela Arrow com o schema calculado
        try:
            table = pa.Table.from_pylist(rows, schema=pa_schema)
        except Exception as e:
            logging.exception("Falha ao construir Table com schema calculado: %s", e)
            # Probe por coluna para identificar onde quebra
            import pyarrow as pa
            for f in pa_schema:
                col_vals = [r.get(f.name) for r in rows]
                sample = next((v for v in col_vals if v is not None), None)
                try:
                    # tente materializar apenas essa coluna com o tipo esperado
                    pa.array(col_vals, type=f.type)
                except Exception as fe:
                    logging.error(
                        "COLUNA PROBLEMÁTICA: '%s' | arrow_type=%s | sample_type=%s | sample_value=%r | erro=%r",
                        f.name, f.type, type(sample).__name__ if sample is not None else None, sample, fe
                    )
            raise  # repropaga para o Dataflow marcar a falha
        
        # extrai a pasta imediatamente após "avro/"
        m = re.search(r'/avro/([^/]+)/', path)
        folder = m.group(1) if m else "unknown"

        # Escreve Parquet particionado por ingestion_date
        dest_path = f"{self.output_prefix}/{folder}/ingestion_date={ingestion_date}/part-{uuid4().hex}.parquet"
        with FileSystems.create(dest_path) as sink:
            pq.write_table(table, sink, compression="snappy")
        logging.info("[PARQUET-WRITTEN] %s", dest_path)
        return []

# ---------------- pipeline ----------------
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_pattern", required=True)
    parser.add_argument("--output_prefix", required=True)   # destino base dos Parquet
    parser.add_argument("--window_duration", default="1h")  # ex.: 30m
    parser.add_argument("--batch_min", type=int, default=500)    # compat
    parser.add_argument("--batch_max", type=int, default=50000)  # compat

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    logging.getLogger().setLevel(logging.INFO)

    win_seconds = parse_duration_to_seconds(known_args.window_duration)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Match AVRO files" >> fileio.MatchFiles(known_args.input_pattern)
            | "To path" >> beam.Map(lambda m: m.path)
            | "Window" >> beam.WindowInto(FixedWindows(win_seconds))
            | "Avro → PyArrow Schema (log) + Parquet" >> beam.ParDo(AvroPathLogPyArrowSchema(known_args.output_prefix))
        )

if __name__ == "__main__":
    run()
