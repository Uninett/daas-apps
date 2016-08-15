#!/bin/sh
spark-submit --class com.github.sparkcaller.SparkCaller \
    --executor-memory 16G \
    --driver-memory 6G \
    sparkcaller-1.0.jar \
    $@
