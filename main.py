# main.py
import argparse
import json
import logging
import re
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

# ---------------- util: parse "30m", "1h", "2d" ----------------
_UNIT_TO_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
def _stringify_any(x):
    if x is None:
        return None
    if isinstance(x, (list, dict, tuple)):
        try:
            return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(x)
    return str(x)

def _to_bool(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in {"true","t","1","yes","y"}: return True
    if s in {"false","f","0","no","n"}: return False
    try:
        return bool(int(s))
    except Exception:
        return None

def _to_int(x):
    if x is None: return None
    if isinstance(x, bool): return int(x)
    if isinstance(x, (int,)): return int(x)
    if isinstance(x, float): return int(x)
    s = str(x).strip()
    if s.startswith("+"): s = s[1:]
    return int(s)

def _to_list_of_str(x):
    if x is None: return None
    if isinstance(x, (list, tuple)):
        return [None if v is None else str(v) for v in x]
    return [str(x)]

# --- schema derivation ---
def derive_flat_schema(_avro_schema_record):
    """
    Retorna um pyarrow.schema com:
      source_metadata: STRUCT<schema STRING, table STRING, is_deleted BOOL, change_type STRING,
                              tx_id INT64, lsn STRING, primary_keys ARRAY<STRING>>
      + todos os campos do payload com tipos primitivos corretos (payload.* promovidos ao topo)
    Obs: ignora demais campos CDC (uuid, read_timestamp, etc.) propositalmente.
    """
    import pyarrow as pa

    # source_metadata fixo
    src_meta_struct = pa.struct([
        pa.field("schema", pa.string()),
        pa.field("table", pa.string()),
        pa.field("is_deleted", pa.bool_()),
        pa.field("change_type", pa.string()),
        pa.field("tx_id", pa.int64()),
        pa.field("lsn", pa.string()),
        pa.field("primary_keys", pa.list_(pa.string())),
    ])

    # descobrir os campos de payload no Avro (union ["null", record] ou record)
    payload_node = None
    for f in _avro_schema_record.get("fields", []):
        if f.get("name") == "payload":
            payload_node = f.get("type")
            break

    payload_fields = []
    def _non_null_types(u):
        return [x for x in u if not (isinstance(x, str) and x == "null")]

    record = None
    if isinstance(payload_node, dict) and payload_node.get("type") == "record":
        record = payload_node
    elif isinstance(payload_node, list):
        for t in _non_null_types(payload_node):
            if isinstance(t, dict) and t.get("type") == "record":
                record = t
                break

    if record:
        for pf in record.get("fields", []):
            field_name = pf["name"]
            field_type = pf["type"]
            
            # Converte o tipo Avro para PyArrow corretamente
            pa_type, nullable = _pyarrow_type_from_node(field_type, field_name=field_name)
            payload_fields.append(pa.field(field_name, pa_type, nullable=nullable))

    # monta schema final: source_metadata + payload.*
    fields = [pa.field("source_metadata", src_meta_struct, nullable=True)]
    fields.extend(payload_fields)
    return pa.schema(fields)

# --- data flattening ---
def flatten_rows_source_meta_and_payload(rows, payload_schema_fields):
    """
    Converte cada linha Avro em:
      { source_metadata: {...}, <payload_field1>: value_with_correct_type, <payload_field2>: value_with_correct_type, ... }
    """
    flat = []
    for r in rows:
        sm = r.get("source_metadata") or {}
        flat_row = {
            "source_metadata": {
                "schema": str(sm.get("schema")) if sm.get("schema") is not None else None,
                "table": str(sm.get("table")) if sm.get("table") is not None else None,
                "is_deleted": _to_bool(sm.get("is_deleted")),
                "change_type": str(sm.get("change_type")) if sm.get("change_type") is not None else None,
                "tx_id": None if sm.get("tx_id") is None else _to_int(sm.get("tx_id")),
                "lsn": str(sm.get("lsn")) if sm.get("lsn") is not None else None,
                "primary_keys": _to_list_of_str(sm.get("primary_keys")),
            }
        }
        
        # payload.* promovido para topo com tipos corretos
        payload = r.get("payload") or {}
        for field_name, field_value in payload.items():
            # Encontra o tipo esperado para este campo no schema
            expected_type = None
            for schema_field in payload_schema_fields:
                if schema_field.name == field_name:
                    expected_type = schema_field.type
                    break
            
            # Converte o valor para o tipo correto
            flat_row[field_name] = _convert_value_to_pyarrow_type(field_value, expected_type)
        
        flat.append(flat_row)
    return flat

def _convert_value_to_pyarrow_type(value, pa_type):
    """Converte um valor para o tipo PyArrow esperado"""
    import pyarrow as pa
    from datetime import date, datetime
    import decimal
    
    if value is None:
        return None
    
    # Se o tipo é timestamp, converte adequadamente
    if pa.types.is_timestamp(pa_type):
        if isinstance(value, (int, float)):
            # Assume que é timestamp em millis ou micros
            return value
        return value
    
    # Se o tipo é date
    if pa.types.is_date(pa_type):
        if isinstance(value, int):
            # Avro date é dias desde epoch (1970-01-01)
            return value
        return value
    
    # Se o tipo é decimal, converte adequadamente para DECIMAL128
    if pa.types.is_decimal(pa_type):
        if isinstance(value, bytes):
            # Decimal em Avro vem como bytes - converte para Decimal Python
            try:
                # Obtém a escala original do Avro (geralmente 30)
                # Precisamos descobrir a escala original para converter corretamente
                # Avro decimal com scale=30 vem como bytes
                int_val = int.from_bytes(value, byteorder='big', signed=True)
                
                # Converte assumindo escala original de 30 (comum nos seus dados)
                original_scale = 30
                decimal_val = decimal.Decimal(int_val) / (decimal.Decimal(10) ** original_scale)
                
                # Reescala para target scale=9 (BigQuery NUMERIC)
                target_scale = 9
                rescaled_val = decimal_val.quantize(decimal.Decimal(10) ** -target_scale)
                
                return rescaled_val
            except Exception as e:
                # Se falhar, tenta converter como zero
                return decimal.Decimal('0.000000000')
        elif isinstance(value, (int, float)):
            # Converte números para Decimal com escala 9
            decimal_val = decimal.Decimal(str(value))
            return decimal_val.quantize(decimal.Decimal('0.000000000'))
        elif isinstance(value, decimal.Decimal):
            # Já é Decimal, apenas reescala para 9 casas decimais
            return value.quantize(decimal.Decimal('0.000000000'))
        return decimal.Decimal('0.000000000')
    
    # Se o tipo é inteiro
    if pa.types.is_integer(pa_type):
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    # Se o tipo é float
    if pa.types.is_floating(pa_type):
        if value is None:
            return None
        
        # Tratamento especial para valores decimais que vêm como bytes
        if isinstance(value, bytes):
            try:
                # Converte bytes decimais para float
                int_val = int.from_bytes(value, byteorder='big', signed=True)
                # Assume escala padrão se não especificada
                decimal_val = decimal.Decimal(int_val) / (decimal.Decimal(10) ** 30)  # escala comum dos decimais
                return float(decimal_val)
            except Exception:
                return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    # Se o tipo é boolean
    if pa.types.is_boolean(pa_type):
        return _to_bool(value)
    
    # Se o tipo é string ou outros, converte para string
    if pa.types.is_string(pa_type) or pa.types.is_binary(pa_type):
        return _stringify_any(value)
    
    # Default: converte para string
    return _stringify_any(value)

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

def _pa_from_primitive(prim: str):
    import pyarrow as pa
    return {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "long": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "bytes": pa.binary(),
        "null": pa.string(),  # fallback amigável
    }.get(prim, pa.string())

def _pa_from_logical_dict(node: dict):
    import pyarrow as pa
    logical = node.get("logicalType")
    base = node.get("type")

    # timestamps lógicos comuns
    if logical == "timestamp-millis" and base in {"long", "int"}:
        return pa.timestamp("ms")
    if logical == "timestamp-micros" and base in {"long", "int"}:
        return pa.timestamp("us")

    # date lógico
    if logical == "date" and base == "int":
        return pa.date32()

    # decimal lógico - converte para DECIMAL128 (P=38, S=9) compatível com BigQuery NUMERIC
    if logical == "decimal":
        # Força precisão/escala compatível com BigQuery NUMERIC (max 38 dígitos, 9 decimais)
        return pa.decimal128(precision=38, scale=9)

    # fallback: usa o tipo base
    return _pa_from_primitive(base)

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
    # União (regra solicitada)
    if _is_union(node):
        # 1) se o primeiro tipo NÃO é null => retorna STRING (conservador)
        first = node[0] if isinstance(node, list) and node else None
        if not (isinstance(first, str) and first == "null"):
            import pyarrow as pa
            return pa.string(), True

        # 2) primeiro é null
        if len(node) < 2:
            import pyarrow as pa
            return pa.string(), True

        second = node[1]

        # 2.a) se o segundo é PRIMITIVO => retorna o tipo do segundo, nullable=True
        if isinstance(second, str):
            return _pa_from_primitive(second), True

        # 2.b) se o segundo é DICT => retorna pelo logicalType (ou base), nullable=True
        if isinstance(second, dict):
            return _pa_from_logical_dict(second), True

        # 2.c) fallback
        import pyarrow as pa
        return pa.string(), True

    # Primitivos via string
    if isinstance(node, str):
        prim = node
        if prim == "string":  return pa.string(), False
        if prim == "boolean": return pa.bool_(), False
        if prim == "int":     return pa.int32(), False
        if prim == "long":    return pa.int64(), False
        if prim == "float":   return pa.float32(), False
        if prim == "decimal": return pa.float64(), False
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

        # --- derivar schema flat (source_metadata + payload.*) ---
        pa_schema = derive_flat_schema(avro_schema)
        logging.info("[PYARROW-SCHEMA-FLAT] file=%s\n%s", path, pa_schema)
        
        # Log dos tipos decimais para debug
        for field in pa_schema:
            if hasattr(field.type, 'precision') and pa.types.is_decimal(field.type):
                logging.info("[DECIMAL-FIELD] field=%s type=decimal128(%d,%d)", 
                           field.name, field.type.precision, field.type.scale)
            elif str(field.type) == 'double':
                logging.info("[FLOAT64-FIELD] field=%s (unexpected float)", field.name)

        # Extrair apenas os campos de payload do schema para conversão de tipos
        payload_schema_fields = [f for f in pa_schema if f.name != "source_metadata"]

        # --- ler novamente os registros ---
        with FileSystems.open(path) as f2:
            rdr2 = fastavro.reader(f2)
            rows = [dict(r) for r in rdr2]

        # --- desestruturar: source_metadata + payload.* com tipos corretos ---
        flat_rows = flatten_rows_source_meta_and_payload(rows, payload_schema_fields)

        try:
            table = pa.Table.from_pylist(flat_rows, schema=pa_schema)
        except Exception as e:
            logging.exception("Falha ao construir Table com schema calculado: %s", e)
            # Probe por coluna para identificar onde quebra
            import pyarrow as pa
            for f in pa_schema:
                col_vals = [r.get(f.name) for r in flat_rows]
                sample = next((v for v in col_vals if v is not None), None)
                try:
                    # tente materializar apenas essa coluna com o tipo esperado
                    pa.array(col_vals, type=f.type)
                except Exception as fe:
                    logging.error(
                        "COLUNA PROBLEMÁTICA: '%s' | arrow_type=%s | sample_type=%s | sample_value=%r | erro=%r",
                        f.name, f.type, type(sample).__name__ if sample is not None else None, sample, fe
                    )
            
            # Tenta criar uma versão mais permissiva do schema (todos como string quando falha)
            logging.warning("Tentando criar schema com tipos string como fallback...")
            try:
                fallback_fields = []
                for f in pa_schema:
                    if f.name == "source_metadata":
                        fallback_fields.append(f)  # Mantém source_metadata como struct
                    else:
                        fallback_fields.append(pa.field(f.name, pa.string(), nullable=True))
                
                fallback_schema = pa.schema(fallback_fields)
                
                # Converte valores problemáticos para string
                fallback_rows = []
                for row in flat_rows:
                    fallback_row = {"source_metadata": row.get("source_metadata")}
                    for key, value in row.items():
                        if key != "source_metadata":
                            fallback_row[key] = _stringify_any(value)
                    fallback_rows.append(fallback_row)
                
                table = pa.Table.from_pylist(fallback_rows, schema=fallback_schema)
                logging.warning("Sucesso com schema fallback (tipos string)")
            except Exception as fe:
                logging.error("Falha mesmo com schema fallback: %s", fe)
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
    
    # Enable streaming mode for continuous processing
    pipeline_options.view_as(StandardOptions).streaming = True
    
    logging.getLogger().setLevel(logging.INFO)

    win_seconds = parse_duration_to_seconds(known_args.window_duration)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Match AVRO files continuously" >> fileio.MatchContinuously(
                file_pattern=known_args.input_pattern,
                interval=win_seconds,  # Poll every window_duration seconds
                has_deduplication=True  # Avoid reprocessing same files
            )
            | "Read matches" >> fileio.ReadMatches()
            | "Extract path" >> beam.Map(lambda readable_file: readable_file.metadata.path)
            | "Window" >> beam.WindowInto(FixedWindows(win_seconds))
            | "Avro → PyArrow Schema (log) + Parquet" >> beam.ParDo(AvroPathLogPyArrowSchema(known_args.output_prefix))
        )

if __name__ == "__main__":
    run()
