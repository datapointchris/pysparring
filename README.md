# pysparring

Put PySpark transform candidates in the ring together, and find out which one wins, on which data,
and why.

pysparring runs two or more transform functions over the same inputs, across datasets, Spark
configurations and clusters. For every combination it reports whether the outputs agree, how the
physical plans differ, and which candidate ran faster. Each timing comes with the statistics to say
whether a difference is real.

Inputs can be Parquet files, DataFrames, or synthetic datasets generated from a profile. A scenario
such as heavy key skew or a share of malformed rows shows whether the winner changes when the data
does.

## License

MIT
