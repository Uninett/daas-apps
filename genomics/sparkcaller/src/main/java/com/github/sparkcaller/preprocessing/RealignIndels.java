package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import org.broadinstitute.gatk.engine.CommandLineGATK;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class RealignIndels extends BaseGATKProgram implements Function<Tuple2<File, File>, File> {

    public RealignIndels(String pathToReference, String extraArgs) {
        super("IndelRealigner", extraArgs);
        setReference(pathToReference);
    }

    public File call(Tuple2<File, File> indelTargetTuple) throws Exception {
        File inputBam = indelTargetTuple._1;
        File intelIntervals = indelTargetTuple._2;

        setInputFile(inputBam.getPath());

        final String newFileName = Utils.removeExtenstion(inputBam.getPath(), "bam") + "-realigned.bam";
        File outputBamFile = new File(newFileName);
        setOutputFile(outputBamFile.getPath());

        addArgument("-targetIntervals", intelIntervals.getPath());

        executeProgram();
        return outputBamFile;
    }
}
