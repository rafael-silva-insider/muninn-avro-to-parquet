import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from fastavro import reader as avro_reader
import io, pyarrow as pa
import uuid, pyarrow as pa, pyarrow.parquet as pq, io
from datetime import datetime

def read_avro_records(readable_file):
    with io.BytesIO(readable_file.open().read()) as f:
        for rec in avro_reader(f):
            yield rec

def infer_arrow_type(v):
    # minimal mapper; expand as you see more types
    if v is None: return pa.null()
    if isinstance(v, bool): return pa.bool_()
    if isinstance(v, int): return pa.int64()
    if isinstance(v, float): return pa.float64()
    if isinstance(v, (bytes, bytearray)): return pa.binary()
    return pa.string()

def merge_schemas(schema_a, schema_b):
    # schemas as dict: {field_name: pa.DataType}
    merged = dict(schema_a)
    for k, t in schema_b.items():
        if k not in merged:
            merged[k] = pa.null()  # placeholder; we’ll reconcile below
        # widen types where needed (keep it simple: coerce conflicts to string)
        if k in schema_a and schema_a[k] != t:
            merged[k] = pa.string()
        else:
            merged[k] = t
    return merged

class SchemaInfer(beam.CombineFn):
    def create_accumulator(self): return {}
    def add_input(self, acc, rec):
        # build a schema from this record
        s = {}
        for k, v in rec.items():
            s[k] = infer_arrow_type(v).with_nullable(True)
        return merge_schemas(acc, s)
    def merge_accumulators(self, accs):
        out = {}
        for a in accs:
            out = merge_schemas(out, a)
        return out
    def extract_output(self, acc): return acc

def to_arrow_row(rec, ordered_fields):
    return [rec.get(f) for f in ordered_fields]

def extract_partitions(rec, ts_field='__source_ts', fmt='%Y-%m-%d', hour_fmt='%H'):
    # choose your partition signals; fallback to ingest time
    now = datetime.utcnow()
    return now.strftime(fmt), now.strftime(hour_fmt)

def build_parquet_schema(merged):
    fields = [pa.field(k, (t if t != pa.null() else pa.string())).with_nullable(True)
              for k, t in merged.items()]
    return pa.schema(fields)

def run(argv=None):
    opts = PipelineOptions(argv)
    gcp = opts.view_as(GoogleCloudOptions)
    std = opts.view_as(StandardOptions)
    std.streaming = True

    input_glob = "gs://insider-lake-ingestion-br/muninn_backups/public_ProductionOrders/*.avro"
    output_prefix = "gs://insider-lake-ingestion-br/muninn_backups/public_ProductionOrders/"

    with beam.Pipeline(options=opts) as p:
        files = (p
                 | "Match" >> fileio.MatchContinuously(file_pattern=input_glob, interval=60)
                 | "ReadMatches" >> fileio.ReadMatches())

        records = files | "ReadAvro" >> beam.FlatMap(read_avro_records)

        # Infer a rolling superset schema
        merged_schema = records | "InferSchema" >> beam.CombineGlobally(SchemaInfer()).as_singleton_view()

        # Write to Parquet with partitioned paths
        def to_parquet_rows(rec, schema_map):
            ordered = list(schema_map.keys())
            date, hour = extract_partitions(rec)
            yield {
                "row": to_arrow_row(rec, ordered),
                "ordered": ordered,
                "date": date,
                "hour": hour,
                "schema": build_parquet_schema(schema_map)
            }

        parquet_rows = records | beam.FlatMap(to_parquet_rows, schema_map=beam.pvalue.AsSingleton(merged_schema))

        # one file per window/partition; for simplicity, shard naturally
        def sink(elem):
            table = pa.Table.from_arrays(
                [pa.array([elem["row"][i]]) for i in range(len(elem["ordered"]))],
                names=elem["ordered"]
            )
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            path = f"{output_prefix}/ingestion_date={elem['date']}/{uuid.uuid4().hex}.parquet"
            with FileSystems.create(path) as f:
                f.write(buf.getvalue())

        _ = parquet_rows | "WriteParquet" >> beam.Map(sink)

if __name__ == "__main__":
    run()