package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.BaseGATKProgram;
import com.github.sparkcaller.utils.Utils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

/*
 * Realign the indels found by the IndelTargetCreator.
 *
 * See:
 * https://www.broadinstitute.org/gatk/guide/article?id=38
 *
 * For more information.
 */
public class RealignIndels extends BaseGATKProgram implements Function<Tuple2<String, File>, File> {

    public RealignIndels(String pathToReference, File indelTargets, String extraArgs) {
        super("IndelRealigner", extraArgs);
        setReference(pathToReference);
        changeArgument("-targetIntervals", indelTargets.getPath());
}

    public File call(Tuple2<String, File> contigTuple) throws Exception {
        String contig = contigTuple._1;
        File inputBam = contigTuple._2;

        setInterval(contig);
        setInputFile(inputBam.getPath());

        final String newFileName = Utils.removeExtenstion(inputBam.getPath(), "bam") + "-realigned.bam";

        File outputBamFile = new File(newFileName);
        setOutputFile(outputBamFile.getPath());

        executeProgram();
        return outputBamFile;
    }
}
