package com.github.sparkcaller.utils;


import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class SAMSplitter implements PairFunction<Tuple2<String, File>, String, File> {
    private File inputFile;

    public SAMSplitter(File inputFile) {
        this.inputFile = inputFile;
    }

    @Override
    public Tuple2<String, File> call(Tuple2<String, File> contigTuple) throws Exception {
        String sequenceName = contigTuple._1;
        File outputFile = contigTuple._2;

        ArrayList<String> arguments = new ArrayList<>();
        arguments.add("view");
        arguments.add("-@");
        arguments.add("32");
        arguments.add("-o");
        arguments.add(outputFile.getPath());
        arguments.add(inputFile.getPath());
        arguments.add(sequenceName);

        int statusCode = Utils.executeResourceBinary("samtools", arguments);

        if (statusCode != 0) {
            System.err.println("Samtools returned status code: " + statusCode + " when attempting to view " + sequenceName);
            return new Tuple2<>(sequenceName, new File(statusCode + ""));
        }

        return new Tuple2<>(sequenceName, outputFile);
    }
}
