### Genomics
This folder contains implementations of genomics related tools.
All of the tools uses [Apache Spark](http://spark.apache.org/) to distribute
the workload.

Currently all of the tools does this by dividing the input into smaller chunks,
and then running the tools on each chunk. By doing so we are able to provide
a familiar interface, while (in most cases) improving the performance of each
tool.
