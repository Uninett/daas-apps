package com.github.sparkcaller.utils;


import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class SAMSplitter implements PairFunction<Tuple2<String, Tuple2<File, File>>, String, Tuple2<File, File>> {
    private String coresPerNode;

    public SAMSplitter(String coresPerNode) {
        this.coresPerNode = coresPerNode;
    }

    @Override
    public Tuple2<String, Tuple2<File, File>> call(Tuple2<String, Tuple2<File, File>> contigTuple) throws Exception {
        String sequenceName = contigTuple._1;
        Tuple2<File, File> inputOutputFile = contigTuple._2;

        File inputFile = inputOutputFile._1;
        File outputFile = new File(inputOutputFile._2.getName());

        ArrayList<String> arguments = new ArrayList<>();
        arguments.add("view");
        arguments.add("-@");
        arguments.add(this.coresPerNode);
        arguments.add("-o");
        arguments.add(outputFile.getPath());
        arguments.add(inputFile.getPath());
        arguments.add(sequenceName);

        int statusCode = MiscUtils.executeResourceBinary("samtools", arguments);

        if (statusCode != 0) {
            System.err.println("Samtools returned status code: " + statusCode + " when attempting to view " + sequenceName);
            return null;
        }

        return new Tuple2<>(sequenceName, new Tuple2<>(inputFile, outputFile));
    }
}
