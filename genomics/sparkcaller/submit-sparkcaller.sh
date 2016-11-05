#!/bin/sh
spark-submit --class com.github.sparkcaller.SparkCaller \
    --conf spark.executor.cores=8 \
    --executor-memory 16G \
    --executor-memory 16G \
    --driver-memory 6G \
    sparkcaller-1.0.jar \
    $@
