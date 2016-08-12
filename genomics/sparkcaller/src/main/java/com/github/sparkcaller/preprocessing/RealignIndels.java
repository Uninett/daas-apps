package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import org.broadinstitute.gatk.engine.CommandLineGATK;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

/*
 * Realign the indels found by the IndelTargetCreator.
 *
 * See:
 * https://www.broadinstitute.org/gatk/guide/article?id=38
 *
 * For more information.
 */
public class RealignIndels extends BaseGATKProgram implements Function<File, File> {

    public RealignIndels(String pathToReference, File indelTargets, String extraArgs) {
        super("IndelRealigner", extraArgs);
        setReference(pathToReference);
        changeArgument("-targetIntervals", indelTargets.getPath());
}

    public File call(File inputBam) throws Exception {
        changeArgument("-I", inputBam.getPath());

        final String newFileName = Utils.removeExtenstion(inputBam.getPath(), "bam") + "-realigned.bam";

        File outputBamFile = new File(newFileName);
        changeArgument("-o", outputBamFile.getPath());

        try {
            executeProgram();
        } catch (org.broadinstitute.gatk.utils.exceptions.ReviewedGATKException e) {
            executeProgram();
        }
        return outputBamFile;
    }
}
