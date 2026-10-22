"""Write every conf key `spark.conf.set` refuses on this Spark to the file named by the first argument.

Spark refuses a static SQL conf, and a core conf it has registered. That is the test
`RuntimeConfig.requireNonStaticConf` makes, asked here of every key in Spark's registry. A key Spark
never registered, such as `spark.hadoop.*` or a custom one, is accepted at runtime.
"""

import sys

from pyspark.sql import SparkSession


def scala_object(spark, package, name):
    node = spark._jvm
    for part in package.split('.'):
        node = getattr(node, part)
    return getattr(getattr(node, name + '$'), 'MODULE$')


def main():
    spark = SparkSession.builder.master('local[1]').getOrCreate()
    sql_conf = scala_object(spark, 'org.apache.spark.sql.internal', 'SQLConf')
    registered = scala_object(spark, 'org.apache.spark.internal.config', 'ConfigEntry').knownConfigs().keySet().toArray()
    refused = sorted(str(key) for key in registered if sql_conf.isStaticConfigKey(key) or not sql_conf.containsConfigKey(key))
    with open(sys.argv[1], 'w') as out:
        out.write('\n'.join(refused) + '\n')
    spark.stop()


main()
