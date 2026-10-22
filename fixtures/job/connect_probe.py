"""Probe a Spark Connect session on localhost:15002, writing under OUT_ROOT/spark-connect-<version>/.

Run by regenerate.sh in the same container as the Connect server.
"""

import contextlib
import io
import json
import os
import sys
import time
import urllib.request

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
OUT = os.path.join(sys.argv[1], f'spark-connect-{spark.version}')
os.makedirs(OUT, exist_ok=True)
facts: dict[str, object] = {'session_class': type(spark).__module__ + '.' + type(spark).__name__}
spark.conf.set('spark.sql.shuffle.partitions', '8')


def write(name, text):
    with open(os.path.join(OUT, name), 'w') as fh:
        fh.write(text)


def explain_captured(df, mode):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


orders = spark.range(0, 500_000).select(
    sf.col('id').alias('order_id'),
    sf.when(sf.rand(7) < 0.3, sf.lit(1)).otherwise((sf.rand(11) * 5000).cast('int') + 1).alias('customer_id'),
    (sf.rand(13) * 100).alias('amount'),
)
customers = spark.range(1, 5001).select(sf.col('id').cast('int').alias('customer_id'), (sf.col('id') % 5).alias('segment'))


def join_then_aggregate():
    return orders.filter(sf.col('amount') > 10).join(customers, 'customer_id').groupBy('segment').agg(sf.sum('amount').alias('total'))


df = join_then_aggregate()
facts['has__jdf'] = hasattr(df, '_jdf')
for mode in ['simple', 'extended', 'codegen', 'cost', 'formatted']:
    write(f'explain-{mode}-initial.txt', explain_captured(df, mode))
facts['executionInfo_before_action'] = repr(df.executionInfo)

rows = df.collect()
info = df.executionInfo
facts['executionInfo_after_collect_type'] = type(info).__name__
if info is not None and info.metrics is not None:
    write('executionInfo-metrics-text.txt', info.metrics.toText())
    nodes = [m.to_dict() if hasattr(m, 'to_dict') else repr(m) for m in info.metrics._metrics] if hasattr(info.metrics, '_metrics') else []
    write('executionInfo-metrics.json', json.dumps(nodes, indent=2, default=str) + '\n')
    facts['executionInfo_node_count'] = len(nodes)
after_collect = explain_captured(df, 'formatted')
write('explain-formatted-after-collect.txt', after_collect)
facts['explain_after_collect_shows_final_plan'] = 'isFinalPlan=true' in after_collect

df2 = join_then_aggregate()
start = time.perf_counter()
df2.write.format('noop').mode('overwrite').save()
facts['noop_write_seconds'] = round(time.perf_counter() - start, 3)
facts['executionInfo_after_noop_write'] = repr(df2.executionInfo)
if df2.executionInfo is not None and df2.executionInfo.metrics is not None:
    write('executionInfo-after-noop-write-text.txt', df2.executionInfo.metrics.toText())
    facts['executionInfo_after_noop_write_nodes'] = len(df2.executionInfo.metrics._metrics)
else:
    facts['executionInfo_after_noop_write_nodes'] = 0

obs = df2.observe('row_counter', sf.count(sf.lit(1)).alias('rows'), sf.sum('total').alias('sum_total'))
obs.collect()
facts['observe_flows'] = obs.executionInfo.flows if obs.executionInfo else None

try:
    with urllib.request.urlopen('http://localhost:4040/api/v1/applications', timeout=10) as resp:
        apps = json.loads(resp.read())
    app_id = apps[0]['id']
    with urllib.request.urlopen(
        f'http://localhost:4040/api/v1/applications/{app_id}/sql?details=true&planDescription=true', timeout=10
    ) as resp:
        sql = json.loads(resp.read())
    facts['rest_reachable_from_client_host'] = True
    facts['rest_sql_descriptions'] = [s['description'][:80] for s in sql][-6:]
except Exception as exc:
    facts['rest_error'] = repr(exc)[:300]

write('facts.json', json.dumps(facts, indent=2, default=str) + '\n')
spark.stop()
