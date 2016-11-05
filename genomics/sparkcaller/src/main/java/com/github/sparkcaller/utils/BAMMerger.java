package com.github.sparkcaller.utils;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BAMMerger implements Function<Tuple2<String ,Iterable<File>>, Tuple2<File, File>> {
    final private String outputName;
    final private String coresPerNode;

    public BAMMerger(String outputName, String coresPerNode) {
        this.outputName = outputName;
        this.coresPerNode = coresPerNode;
    }
    @Override
    public Tuple2<File, File> call(Tuple2<String, Iterable<File>> tuple2Iterator) throws Exception {
        List<File> filesToMerge = new ArrayList<>();

        for (File inputBAMFile : tuple2Iterator._2) {
            filesToMerge.add(inputBAMFile);
        }

        File inputFile = filesToMerge.get(0);
        String baseOutputName = inputFile.getName();

        String[] outputFileNameParts = baseOutputName.split("-");
        String outputFileName = outputFileNameParts[0];

        for (int i = 1; i < outputFileNameParts.length-2; i++) {
            outputFileName += "-" + outputFileNameParts[i];
        }

        outputFileName += "-" + this.outputName;

        File outputFile = SAMFileUtils.mergeBAMFiles(filesToMerge, outputFileName, this.coresPerNode);
        return new Tuple2<>(inputFile, outputFile);
    }
}
