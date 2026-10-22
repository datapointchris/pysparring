"""Capture a classic Spark session's plans, timings and REST answers under OUT_ROOT/spark-<version>/.

Run by regenerate.sh inside an apache/spark image, with sparkMeasure on the classpath.
"""

import difflib
import inspect
import json
import os
import random
import re
import statistics
import sys
import time
import urllib.request

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql import Window

OUT_ROOT = sys.argv[1]
MODES = ['simple', 'extended', 'codegen', 'cost', 'formatted']

spark = (
    SparkSession.builder.appName('pysparring-fixtures')
    .master('local[4]')
    .config('spark.sql.shuffle.partitions', '8')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.ui.enabled', 'true')
    .config('spark.ui.port', '4040')
    .config('spark.driver.memory', '2g')
    .getOrCreate()
)
VERSION = spark.version
OUT = os.path.join(OUT_ROOT, f'spark-{VERSION}')
os.makedirs(OUT, exist_ok=True)
jvm = spark.sparkContext._jvm
facts: dict[str, object] = {'spark_version': VERSION, 'python': sys.version.split()[0]}


def write(name, text):
    with open(os.path.join(OUT, name), 'w') as fh:
        fh.write(text if text.endswith('\n') else text + '\n')


def read(name):
    with open(os.path.join(OUT, name)) as fh:
        return fh.read()


def explain_string(df, mode):
    return jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


# The first draft of the plan-normalization table. The plan-diff code carries the anchored one.
NORMALIZERS = [
    (re.compile(r'#\d+L?'), '#N'),
    (re.compile(r'plan_id=\d+'), 'plan_id=N'),
    (re.compile(r'\[id=#?\d+\]'), '[id=N]'),
    (re.compile(r'^\(\d+\) ', re.M), '(N) '),
    (re.compile(r' \(\d+\)$', re.M), ' (N)'),
    (re.compile(r'(Result|Shuffle|Broadcast|TableCache)QueryStage \d+'), r'\1QueryStage N'),
    (re.compile(r'WholeStageCodegen \(\d+\)'), 'WholeStageCodegen (N)'),
    (re.compile(r'codegen id : \d+'), 'codegen id : N'),
    (re.compile(r'\*\(\d+\)'), '*(N)'),
    (re.compile(r'file:[^\s,\]]+'), '<path>'),
    (re.compile(r'subquery#?\d+'), 'subqueryN'),
    (re.compile(r'sizeInBytes=[\d.]+ ?[KMGTPE]?i?B'), 'sizeInBytes=<size>'),
    (re.compile(r'rowCount=[\d.E+]+'), 'rowCount=<n>'),
]


def normalize(plan):
    for pattern, repl in NORMALIZERS:
        plan = pattern.sub(repl, plan)
    return plan


def rest(path):
    base = spark.sparkContext.uiWebUrl
    app_id = spark.sparkContext.applicationId
    url = f'{base}/api/v1/applications/{app_id}/{path}'
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


# A skewed fact and a small dimension, round-tripped through Parquet so scans show pushdown.
data_dir = '/tmp/pysparring-data'
orders = spark.range(0, 2_000_000).select(
    sf.col('id').alias('order_id'),
    sf.when(sf.rand(7) < 0.3, sf.lit(1)).otherwise((sf.rand(11) * 5000).cast('int') + 1).alias('customer_id'),
    (sf.rand(13) * 100).alias('amount'),
    sf.date_add(sf.lit('2026-01-01'), (sf.rand(17) * 365).cast('int')).alias('order_date'),
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

# Explain in every mode before execution, the JSON plans, then the final plans after collect.
for cfg_name, cfg in CONFIGS.items():
    for k, v in cfg.items():
        spark.conf.set(k, v)
    for t_name, fn in TRANSFORMS.items():
        df = fn(orders, customers)
        for mode in MODES:
            write(f'explain-{t_name}-{cfg_name}-{mode}-initial.txt', explain_string(df, mode))
        qe = df._jdf.queryExecution()
        for plan_name, plan in [('analyzed', qe.analyzed()), ('optimized', qe.optimizedPlan()), ('executed', qe.executedPlan())]:
            try:
                write(f'json-{t_name}-{cfg_name}-{plan_name}.json', plan.prettyJson())
            except Exception as exc:  # toJSON fails on some nodes, and the failure is itself a finding
                write(f'json-{t_name}-{cfg_name}-{plan_name}.error.txt', repr(exc)[:4000])
        # Collecting finalizes the AdaptiveSparkPlanExec inside this QueryExecution.
        rows = df.collect()
        write(f'explain-{t_name}-{cfg_name}-formatted-final.txt', explain_string(df, 'formatted'))
        write(f'explain-{t_name}-{cfg_name}-simple-final.txt', explain_string(df, 'simple'))
        result = [r.asDict() for r in sorted(rows, key=lambda r: r['segment'])]
        write(f'result-{t_name}-{cfg_name}.json', json.dumps(result, indent=2, default=str))

# The same transform built twice: are the normalized plans byte-identical?
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', str(10 * 1024 * 1024))
a1 = explain_string(join_then_aggregate(orders, customers), 'formatted')
a2 = explain_string(join_then_aggregate(orders, customers), 'formatted')
facts['raw_plans_identical_across_two_builds'] = a1 == a2
facts['normalized_plans_identical_across_two_builds'] = normalize(a1) == normalize(a2)
if normalize(a1) != normalize(a2):
    write('normalize-residue.diff', ''.join(difflib.unified_diff(normalize(a1).splitlines(True), normalize(a2).splitlines(True))))

# The diffs a tool would show.
for mode in ['simple', 'formatted']:
    for cfg_name in CONFIGS:
        a = normalize(read(f'explain-join_then_aggregate-{cfg_name}-{mode}-final.txt'))
        b = normalize(read(f'explain-aggregate_then_join-{cfg_name}-{mode}-final.txt'))
        diff = difflib.unified_diff(a.splitlines(True), b.splitlines(True), 'join_then_aggregate', 'aggregate_then_join')
        write(f'diff-transforms-{cfg_name}-{mode}-final.diff', ''.join(diff))
on = normalize(read('explain-join_then_aggregate-broadcast_on-simple-final.txt'))
off = normalize(read('explain-join_then_aggregate-broadcast_off-simple-final.txt'))
write(
    'diff-configs-join_then_aggregate-simple-final.diff',
    ''.join(difflib.unified_diff(on.splitlines(True), off.splitlines(True), 'broadcast_on', 'broadcast_off')),
)

# Sequential timings: a noop sink, one warm-up, five measured, the cache cleared each time.
timings = []
for cfg_name, cfg in CONFIGS.items():
    for k, v in cfg.items():
        spark.conf.set(k, v)
    for t_name, fn in TRANSFORMS.items():
        samples = []
        for rep in range(6):
            spark.catalog.clearCache()
            spark.sparkContext.setJobGroup(f'{t_name}:{cfg_name}:{rep}', f'{t_name} {cfg_name} rep {rep}')
            start = time.perf_counter()
            fn(orders, customers).write.format('noop').mode('overwrite').save()
            elapsed = time.perf_counter() - start
            if rep > 0:
                samples.append(elapsed)
        timings.append({'transform': t_name, 'config': cfg_name, 'samples_s': samples, 'median_s': statistics.median(samples)})
write('timings.json', json.dumps(timings, indent=2))

# The same arms interleaved: every round runs each arm once, in a shuffled order.
rng = random.Random(42)
arms = [(c, t) for c in CONFIGS for t in TRANSFORMS]
interleaved: dict[str, list[float]] = {f'{t}:{c}': [] for c, t in arms}
for rnd in range(7):
    order = arms[:]
    rng.shuffle(order)
    for cfg_name, t_name in order:
        for k, v in CONFIGS[cfg_name].items():
            spark.conf.set(k, v)
        spark.catalog.clearCache()
        start = time.perf_counter()
        TRANSFORMS[t_name](orders, customers).write.format('noop').mode('overwrite').save()
        elapsed = time.perf_counter() - start
        if rnd > 0:
            interleaved[f'{t_name}:{cfg_name}'].append(elapsed)
write(
    'timings-interleaved.json',
    json.dumps({k: {'samples_s': v, 'median_s': statistics.median(v)} for k, v in interleaved.items()}, indent=2),
)

# The REST API in local mode: SQL executions with plan description and node metrics, and stages.
try:
    sql = rest('sql?details=true&planDescription=true&length=1000')
    write('rest-sql-executions.json', json.dumps(sql, indent=2))
    facts['rest_sql_execution_count'] = len(sql)
    facts['rest_sql_execution_keys'] = sorted(sql[-1].keys()) if sql else []
    stages = rest('stages?details=false')
    write('rest-stages.json', json.dumps(stages[:20], indent=2))
    facts['rest_stage_keys'] = sorted(stages[0].keys()) if stages else []
except Exception as exc:
    facts['rest_error'] = repr(exc)

# The multiset symmetric difference of the two outputs, and the float-tolerance trap.
x = join_then_aggregate(orders, customers)
y = aggregate_then_join(orders, customers)
facts['exceptAll_only_in_join_then_aggregate'] = x.exceptAll(y).count()
facts['exceptAll_only_in_aggregate_then_join'] = y.exceptAll(x).count()
xr = {r['segment']: r for r in x.collect()}
yr = {r['segment']: r for r in y.collect()}
facts['max_abs_total_difference'] = max(abs(xr[s]['total'] - yr[s]['total']) for s in xr)
facts['counts_equal'] = all(xr[s]['n'] == yr[s]['n'] for s in xr)
write('explain-exceptAll-symmetric-diff-formatted.txt', explain_string(x.exceptAll(y), 'formatted'))

# One plan per heuristic rule's shape.
specimens = {}
py_udf = sf.udf(lambda a: 'big' if a > 50 else 'small', 'string')
specimens['python_udf_BatchEvalPython'] = orders.select('order_id', py_udf('amount').alias('size'))
specimens['cartesian_product'] = customers.alias('a').crossJoin(customers.alias('b'))
specimens['window_without_partition'] = orders.select('order_id', sf.row_number().over(Window.orderBy('amount')).alias('rn'))
specimens['repeated_scan_union'] = orders.filter('amount > 90').unionByName(orders.filter('amount < 10'))
orders.write.mode('overwrite').partitionBy('order_date').parquet(f'{data_dir}/orders_by_date')
by_date = spark.read.parquet(f'{data_dir}/orders_by_date')
specimens['partition_filter_and_pushed_filter'] = by_date.filter((sf.col('order_date') == '2026-03-01') & (sf.col('amount') > 50))
specimens['filter_on_cast_blocks_pushdown'] = orders.filter(sf.col('amount').cast('string').startswith('9'))
specimens['skew_join_smj'] = orders.join(
    orders.select('customer_id').distinct().withColumnRenamed('customer_id', 'cid'), sf.col('customer_id') == sf.col('cid')
)
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
for name, df in specimens.items():
    write(f'specimen-{name}-formatted-initial.txt', explain_string(df, 'formatted'))
    try:
        df.write.format('noop').mode('overwrite').save()
    except Exception as exc:
        write(f'specimen-{name}-error.txt', repr(exc)[:4000])
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', str(10 * 1024 * 1024))

# A noop write's final plan is not in df.queryExecution, so it is read back from the status store.
try:
    store = spark._jsparkSession.sharedState().statusStore()
    execs = store.executionsList()
    last = execs.apply(execs.size() - 1)
    write('statusstore-last-execution-plan.txt', last.physicalPlanDescription())
    facts['statusstore_last_description'] = last.description()[:200]
except Exception as exc:
    facts['statusstore_error'] = repr(exc)[:500]

# What the built-in assertions take.
try:
    from pyspark.testing import assertDataFrameEqual
    from pyspark.testing import assertSchemaEqual

    facts['assertDataFrameEqual_signature'] = str(inspect.signature(assertDataFrameEqual))
    facts['assertSchemaEqual_signature'] = str(inspect.signature(assertSchemaEqual))
except Exception as exc:
    facts['assert_error'] = repr(exc)

# sparkMeasure's stage metrics over one run.
try:
    from sparkmeasure import StageMetrics

    sm = StageMetrics(spark)
    sm.begin()
    join_then_aggregate(orders, customers).write.format('noop').mode('overwrite').save()
    sm.end()
    try:
        write('sparkmeasure-stage-report.txt', sm.report())
    except Exception as exc:
        facts['sparkmeasure_report_error'] = repr(exc)[:300]
    try:
        agg = sm.aggregate_stagemetrics()
        write('sparkmeasure-aggregate.json', json.dumps(agg, indent=2, default=str))
        facts['sparkmeasure_aggregate_keys'] = sorted(agg.keys())
    except Exception as exc:
        facts['sparkmeasure_aggregate_error'] = repr(exc)[:300]
except Exception as exc:
    facts['sparkmeasure_error'] = repr(exc)[:500]

write('facts.json', json.dumps(facts, indent=2, default=str))
spark.stop()
