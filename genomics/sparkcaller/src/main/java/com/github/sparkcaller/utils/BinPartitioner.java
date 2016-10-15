package com.github.sparkcaller.utils;

import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Map;

public class BinPartitioner extends Partitioner implements Serializable {
    final private int numPartitions;
    final private Map<String, Integer> contigMapping;

    public BinPartitioner(int numPartitions, Map<String, Integer> contigMapping) {
        this.numPartitions = numPartitions;
        this.contigMapping = contigMapping;
    }

    @Override
    public int numPartitions() {
        return this.numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        String sequenceName = (String) key;
        return this.contigMapping.get(sequenceName);
    }
}
