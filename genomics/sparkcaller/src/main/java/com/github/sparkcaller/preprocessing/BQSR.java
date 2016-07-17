package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class BQSR extends BaseGATKProgram implements Function<Tuple2<File, File>, File> {
    public BQSR(String pathToReference, String extraArgsString) {
        super("PrintReads", extraArgsString);
        setReference(pathToReference);
    }

    public File call(Tuple2<File, File> bqsrTablePair) throws Exception {
        File bamFile = bqsrTablePair._1;
        File bqsrTable = bqsrTablePair._2;

        setInputFile(bamFile.getPath());
        addArgument("-BQSR", bqsrTable.getPath());

        String outputBamFilename = Utils.removeExtenstion(bamFile.getPath(), "bam") + "-bqsr.bam";
        File outputBam = new File(outputBamFilename);

        setOutputFile(outputBam.getPath());

        executeProgram();
        return outputBam;
    }
}
