package com.github.sparkcaller.utils;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.nio.file.Paths;

public class FileMover implements Function<Tuple2<File, File>, File> {
    final private String outputFolderPostfix;

    public FileMover(String outputFolderPostfix) {
        this.outputFolderPostfix = outputFolderPostfix;
    }

    @Override
    public File call(Tuple2<File, File> inputOutputTuple) throws Exception {
        File fileToMove = inputOutputTuple._2;
        File newLocation = inputOutputTuple._1.getParentFile();

        if (newLocation == null || !newLocation.getName().equals(outputFolderPostfix)) {
             newLocation = new File(newLocation, outputFolderPostfix);
        }

        return MiscUtils.moveToDir(fileToMove, newLocation.getPath());
    }
}
