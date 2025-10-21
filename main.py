# main.py
import argparse
import io
import uuid
import re
import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
import json
import logging

from datetime import datetime, timezone
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.transforms.util import BatchElements
from fastavro import reader as avro_reader

logging.getLogger().setLevel(logging.INFO)

def _parse_duration_to_seconds(text: str) -> int:
    """
    Converte '10m', '1h', '30s' para segundos (int). Inteiro = segundos.
    """
    if isinstance(text, int):
        return text
    m = re.fullmatch(r"(\d+)\s*([smh]?)", str(text).strip(), re.IGNORECASE)
    if not m:
        raise ValueError(f"window_duration inválido: {text}")
    value, unit = int(m.group(1)), m.group(2).lower()
    return value * 3600 if unit == "h" else value * 60 if unit == "m" else value

def _normalize_sort_keys_to_strings(v):
    """Converte sort_keys (array de union<string,long>) para list<string>."""
    if v is None:
        return None
    try:
        it = list(v)
    except TypeError:
        return None
    out = []
    for item in it:
        if item is None:
            out.append(None)
        elif isinstance(item, (bytes, bytearray)):
            try:
                out.append(item.decode("utf-8"))
            except Exception:
                out.append(item.decode("latin1", errors="replace"))
        elif isinstance(item, bool):
            out.append("1" if item else "0")
        else:
            out.append(str(item))
    return out

def _diagnose_arrow_columns(rows, sample_n=5):
    import pyarrow as pa
    import logging

    # universo de chaves
    keys = set()
    for r in rows:
        keys.update(r.keys())

    logging.warning("[ARROW DIAG] iniciando diagnóstico de %d colunas", len(keys))

    for k in sorted(keys):
        vals = [r.get(k, None) for r in rows]
        try:
            arr = pa.array(vals)  # deixa o Arrow inferir
            py_types = {type(v).__name__ for v in vals if v is not None}
            logging.warning(
                "[ARROW DIAG][OK] coluna=%s tipo_inferido=%s tipos_python=%s amostra=%s",
                k, arr.type, py_types, [v for v in vals if v is not None],
            )
        except Exception as e:
            py_types = {type(v).__name__ for v in vals if v is not None}
            logging.error(
                "[ARROW DIAG][ERRO] coluna=%s erro=%s tipos_python=%s amostra=%s",
                k, e, py_types, [v for v in vals if v is not None],
            )
            
def _pa_array_or_string(col_name, values, sample_n=5):
    """Tenta inferir a coluna; se falhar, loga e converte toda a coluna para string."""
    try:
        return pa.array(values)  # inferência normal
    except Exception as e:
        # log útil para depuração
        py_types = {type(v).__name__ for v in values if v is not None}
        logging.warning(
            "[ARROW FALLBACK] coluna=%s -> string; erro=%s; tipos_python=%s; amostra=%s",
            col_name, e, py_types, [v for v in values if v is not None][:sample_n],
        )
        # preserva None; força string para os demais
        coerced = [None if v is None else str(v) for v in values]
        return pa.array(coerced, type=pa.string())

def _table_with_column_fallback(rows):
    """Constroi uma pa.Table coluna a coluna com fallback para string nas colunas problemáticas."""
    if not rows:
        return pa.table({})
    # conjunto de chaves
    keys = set().union(*(r.keys() for r in rows))
    columns = {}
    for k in sorted(keys):
        values = [r.get(k, None) for r in rows]
        columns[k] = _pa_array_or_string(k, values)
    return pa.table(columns)

class LogAvroSchemaDoFn(beam.DoFn):
    """Lê o schema Avro do arquivo e imprime no log (DEBUG/INFO)."""
    def process(self, file_path: str):
        with FileSystems.open(file_path) as f:
            avro = avro_reader(f)
            schema = avro.schema  # dict
            logging.info("AVRO SCHEMA [%s]:\n%s",
                         file_path,
                         json.dumps(schema, indent=2, ensure_ascii=False))
        # opcional: emitir o path pra encadear com outros steps de teste
        yield file_path

class AttachIngestionDateDoFn(beam.DoFn):
    """
    Sempre usa *processing time* (agora em UTC) para gerar a partição.
    Ignora completamente event-time e início de janela para robustez.
    """
    def process(self, element):
        date_str = datetime.now(timezone.utc).date().isoformat()  # YYYY-MM-DD
        # Mantemos chave = ingestion_date para facilitar partição na escrita
        yield (date_str, element)


class WriteParquetBatchesDoFn(beam.DoFn):
    """
    Recebe lotes (lista) de pares (ingestion_date, elemento).
    Por robustez, consolidamos no buffer por chave e escrevemos um arquivo
    por chave por lote, evitando misturar datas distintas no mesmo arquivo.
    """
    def __init__(self, output_prefix: str):
        self.output_prefix = output_prefix.rstrip("/") + "/"

    def process(self, batch_of_pairs):
        # batch_of_pairs é uma lista de (ingestion_date, element) vinda do BatchElements
        if not batch_of_pairs:
            return

        # Agrupa em memória por ingestion_date
        buckets = {}
        for date_key, elem in batch_of_pairs:
            buckets.setdefault(date_key, []).append(elem)

        # Para cada chave (ingestion_date), escreve um arquivo parquet
        for date_key, rows in buckets.items():
            if not rows:
                continue
            # depois: normaliza sort_keys -> list<string> e mantém o resto automático
            normalized_rows = []
            for r in rows:
                r2 = dict(r)  # cópia rasa
                if "sort_keys" in r2:
                    r2["sort_keys"] = _normalize_sort_keys_to_strings(r2.get("sort_keys"))
                normalized_rows.append(r2)

            table = _table_with_column_fallback(normalized_rows)

            shard = uuid.uuid4().hex[:12]
            dest_path = f"{self.output_prefix}ingestion_date={date_key}/part-{shard}.parquet"

            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            with FileSystems.create(dest_path) as fh:
                fh.write(buf.read())

            # métrica opcional
            beam.metrics.Metrics.counter("io", "files_written").inc()
            yield dest_path


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_pattern", required=True)
    parser.add_argument("--output_prefix", required=True)
    parser.add_argument("--window_duration", default="30m")
    parser.add_argument("--batch_min", type=int, default=500)
    parser.add_argument("--batch_max", type=int, default=50000)

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    window_secs = _parse_duration_to_seconds(known_args.window_duration)
    
    # with beam.Pipeline(options=pipeline_options) as p:
    #     (
    #         p
    #         | "MatchAvroFiles" >> fileio.MatchFiles(known_args.input_pattern)
    #         | "GetPaths" >> beam.Map(lambda m: m.path)          # m é FileMetadata
    #         | "TakeFew" >> beam.combiners.Sample.FixedSizeGlobally(3)  # logar até 3 arquivos
    #         | "Flatten" >> beam.FlatMap(lambda xs: xs or [])    # xs pode ser None
    #         | "LogSchemas" >> beam.ParDo(LogAvroSchemaDoFn())
    #     )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromAvro" >> beam.io.avroio.ReadFromAvro(known_args.input_pattern)
            # Janela continua existindo apenas para limitar volume/concorrência;
            # ela NÃO determina a partição de saída.
            | "Window" >> beam.WindowInto(beam.window.FixedWindows(window_secs))
            # Partição *sempre* por processing time (UTC)
            | "AttachIngestionDate" >> beam.ParDo(AttachIngestionDateDoFn())
            # Faz lotes para reduzir quantidade de arquivos; como há chave no elemento,
            # o DoFn de escrita separa por chave novamente antes de gravar.
            | "Batch" >> BatchElements(
                min_batch_size=known_args.batch_min,
                max_batch_size=known_args.batch_max,
            )
            | "WriteParquet" >> beam.ParDo(WriteParquetBatchesDoFn(known_args.output_prefix))
            | "LogMetric" >> beam.Map(lambda _: beam.metrics.Metrics.counter("io", "files_write_ops").inc())
        )


if __name__ == "__main__":
    run()
