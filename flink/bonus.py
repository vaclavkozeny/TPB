from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
import re
import orjson

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)


fs = FileSource.for_record_stream_format(StreamFormat.text_line_format(),"/files/articles-clear.jsonl").process_static_file_set().build()
ds = env.from_source(source = fs, watermark_strategy = WatermarkStrategy.for_monotonous_timestamps(), source_name = "jsonl")

ds = ds.rebalance()

def extract_content(line):
    try:
        obj = orjson.loads(line)
        content = obj.get("content", "")
        if content:
            yield content
    except Exception:
        pass

ds = ds.flat_map(extract_content, output_type=Types.STRING())

ds = ds.flat_map(lambda s: s.lower().split(), output_type=Types.STRING()) \
       .filter(lambda i: i and re.match(r'^[a-zěščřžýáíéúů]$', i[0])) \
       .map(lambda i: (i[0], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
       .key_by(lambda i: i[0]) \
       .reduce(lambda a, b: (a[0], a[1] + b[1]))

encoder = Encoder.simple_string_encoder()

sink = FileSink.for_row_format("/files/out/", encoder) \
    .with_output_file_config(
        OutputFileConfig.builder()
            .with_part_prefix("part")
            .with_part_suffix(".txt")
            .build()
    ).build()

ds.map(lambda x: f"{x[0]} {x[1]}", output_type=Types.STRING()) \
  .sink_to(sink)

env.execute()