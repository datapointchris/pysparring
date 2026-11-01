"""Capture a classic Spark session's formatted plans and interleaved run times under OUT_ROOT/spark-<version>/.

Run by regenerate.sh under spark-submit inside an apache/spark image.
"""

import contextlib
import io
import json
import os
import random
import sys
import time

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

OUT_ROOT = sys.argv[1]
ROUNDS = 6

spark = (
    SparkSession.builder.appName('pysparring-fixtures')
    .master('local[4]')
    .config('spark.sql.shuffle.partitions', '8')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.driver.memory', '2g')
    .getOrCreate()
)
OUT = os.path.join(OUT_ROOT, f'spark-{spark.version}')
os.makedirs(OUT, exist_ok=True)


def write(name, text):
    with open(os.path.join(OUT, name), 'w') as fh:
        fh.write(text)


def explain_formatted(df):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode='formatted')
    return buffer.getvalue()


# A skewed fact and a small dimension, round-tripped through Parquet so scans show pushdown.
data_dir = '/tmp/pysparring-data'
orders = spark.range(0, 2_000_000).select(
    sf.col('id').alias('order_id'),
    sf.when(sf.rand(7) < 0.3, sf.lit(1)).otherwise((sf.rand(11) * 5000).cast('int') + 1).alias('customer_id'),
    (sf.rand(13) * 100).alias('amount'),
)
customers = spark.range(1, 5001).select(
    sf.col('id').cast('int').alias('customer_id'),
    sf.element_at(
        sf.array(*[sf.lit(s) for s in ['retail', 'smb', 'enterprise', 'gov', 'edu']]),
        (sf.col('id') % 5 + 1).cast('int'),
    ).alias('segment'),
)
orders.write.mode('overwrite').parquet(f'{data_dir}/orders')
customers.write.mode('overwrite').parquet(f'{data_dir}/customers')
orders = spark.read.parquet(f'{data_dir}/orders')
customers = spark.read.parquet(f'{data_dir}/customers')


def join_then_aggregate(orders, customers):
    return (
        orders.filter(sf.col('amount') > 10)
        .join(customers, 'customer_id')
        .groupBy('segment')
        .agg(sf.sum('amount').alias('total'), sf.count('*').alias('n'))
    )


def aggregate_then_join(orders, customers):
    per_customer = (
        orders.filter(sf.col('amount') > 10).groupBy('customer_id').agg(sf.sum('amount').alias('total'), sf.count('*').alias('n'))
    )
    return per_customer.join(customers, 'customer_id').groupBy('segment').agg(sf.sum('total').alias('total'), sf.sum('n').alias('n'))


TRANSFORMS = {'join_then_aggregate': join_then_aggregate, 'aggregate_then_join': aggregate_then_join}
CONFIGS = {
    'broadcast_on': {'spark.sql.autoBroadcastJoinThreshold': str(10 * 1024 * 1024)},
    'broadcast_off': {'spark.sql.autoBroadcastJoinThreshold': '-1'},
}


def apply(config):
    for key, value in CONFIGS[config].items():
        spark.conf.set(key, value)


for config in CONFIGS:
    apply(config)
    for name, transform in TRANSFORMS.items():
        write(f'explain-{name}-{config}.txt', explain_formatted(transform(orders, customers)))
# A second build of the same transform under the same conf, whose ids differ while its plan does not.
write('explain-join_then_aggregate-broadcast_off-rebuilt.txt', explain_formatted(join_then_aggregate(orders, customers)))


def run_once(name, config):
    apply(config)
    df = TRANSFORMS[name](orders, customers)
    spark.catalog.clearCache()
    started = time.perf_counter()
    df.write.format('noop').mode('overwrite').save()
    return (time.perf_counter() - started) * 1000


# The schedule compare_plans uses: rounds in a shuffled order, the first WARMUP_ROUNDS discarded.
WARMUP_ROUNDS = 2
arms = [(name, config) for config in CONFIGS for name in TRANSFORMS]
samples: dict[str, list[float]] = {f'{name}:{config}': [] for name, config in arms}
schedule = random.Random(0)
for round_number in range(1 - WARMUP_ROUNDS, ROUNDS + 1):
    schedule.shuffle(arms)
    for name, config in arms:
        elapsed = run_once(name, config)
        if round_number > 0:
            samples[f'{name}:{config}'].append(round(elapsed, 1))
write('timings-interleaved.json', json.dumps(samples, indent=2) + '\n')
spark.stop()
