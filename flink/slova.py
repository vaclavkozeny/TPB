from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.connectors import FileSource, StreamFormat
import re

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

fs = FileSource.for_record_stream_format(StreamFormat.text_line_format(),"/files/stream").process_static_file_set().build()
ds = env.from_source(source = fs, watermark_strategy = WatermarkStrategy.for_monotonous_timestamps(), source_name = "fs")

ds = ds.flat_map(lambda s: s.lower().split(), output_type=Types.STRING()) \
       .filter(lambda i: i and re.match(r'^[a-z]$', i[0])) \
       .map(lambda i: (i[0], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
       .key_by(lambda i: i[0]) \
       .reduce(lambda a, b: (a[0], a[1] + b[1]))

ds.print()
env.execute()