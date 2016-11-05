package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.BaseGATKProgram;
import com.github.sparkcaller.utils.MiscUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

/*
 * Minimize the amount of mismatching bases across all reads.
 *
 * See:
 * https://www.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_indels_RealignerTargetCreator.php
 *
 * For more information.
 *
 */
public class IndelTargetCreator extends BaseGATKProgram implements Function<File, Tuple2<File, File>> {

    public IndelTargetCreator(String pathToReference, String extraArgsString, String threads) {
        super("RealignerTargetCreator", extraArgsString);
        setReference(pathToReference);
        addArgument("-nt", threads);
    }

    public File createTargets(File bamFile) throws Exception {
        setInputFile(bamFile.getPath());

        final String outputIntervalsFilename = MiscUtils.removeExtenstion(bamFile.getName(), "bam") + "-target.intervals";
        File outputIntervalsFile = new File(outputIntervalsFilename);
        setOutputFile(outputIntervalsFile.getPath());

        executeProgram();
        return outputIntervalsFile;
    }

    @Override
    public Tuple2<File, File> call(File inputFile) throws Exception {
        File targets = this.createTargets(inputFile);

        return new Tuple2<>(inputFile, targets);
    }
}
