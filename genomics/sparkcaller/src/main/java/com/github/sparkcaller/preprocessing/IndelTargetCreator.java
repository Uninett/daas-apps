package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.BaseGATKProgram;
import com.github.sparkcaller.utils.Utils;
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
    private String outputFolder;

    public IndelTargetCreator(String pathToReference, String outputFolder, String extraArgsString, String coresPerNode) {
        super("RealignerTargetCreator", extraArgsString);
        setReference(pathToReference);
        this.outputFolder = outputFolder;
    }

    public File createTargets(File bamFile) throws Exception {
        setInputFile(bamFile.getPath());

        final String outputIntervalsFilename = Utils.removeExtenstion(bamFile.getPath(), "bam") + "-target.intervals";
        File outputIntervalsFile = new File(outputIntervalsFilename);
        setOutputFile(outputIntervalsFile.getPath());

        executeProgram();
        return Utils.moveToDir(outputIntervalsFile, this.outputFolder);
    }

    @Override
    public Tuple2<File, File> call(File bamFile) throws Exception {
        setInputFile(bamFile.getPath());

        final String outputIntervalsFilename = Utils.removeExtenstion(bamFile.getPath(), "bam") + "-target.intervals";
        File outputIntervalsFile = new File(outputIntervalsFilename);
        setOutputFile(outputIntervalsFile.getPath());

        executeProgram();
        return new Tuple2<>(bamFile, outputIntervalsFile);
    }
}
