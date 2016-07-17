package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import org.broadinstitute.gatk.engine.CommandLineGATK;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class RealignIndels implements Function<Tuple2<File, File>, File> {
    final private String pathToReference;

    public RealignIndels(String pathToReference) {
        this.pathToReference = pathToReference;
    }
    public File call(Tuple2<File, File> indelTargetTuple) throws Exception {
        File inputBam = indelTargetTuple._1;
        File intelIntervals = indelTargetTuple._2;

        ArrayList<String> alignerArgs = new ArrayList<String>();
        alignerArgs.add("-T");
        alignerArgs.add("IndelRealigner");

        alignerArgs.add("-R");
        alignerArgs.add(this.pathToReference);

        alignerArgs.add("-I");
        alignerArgs.add(inputBam.getPath());

        final String ext = "bam";
        final String newFileName = Utils.removeExtenstion(inputBam.getPath(), "bam") + "-realigned.bam";
        File outputBamFile = new File(newFileName);

        alignerArgs.add("-targetIntervals");
        alignerArgs.add(intelIntervals.getPath());

        alignerArgs.add("-o");
        alignerArgs.add(outputBamFile.getPath());

        CommandLineGATK.start(new CommandLineGATK(), alignerArgs.toArray(new String[0]));

        return outputBamFile;
    }
}
