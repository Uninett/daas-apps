package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

/*
 * Minimize the amount of mismatching bases across all reads.
 *
 * See:
 * https://www.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_indels_RealignerTargetCreator.php
 *
 * For more information.
 *
 */
public class IndelTargetCreator extends BaseGATKProgram {
    public IndelTargetCreator(String pathToReference, String extraArgsString, String coresPerNode) {
        super("RealignerTargetCreator", extraArgsString);
        setReference(pathToReference);
        addArgument("-nt", coresPerNode); // The target creator is better optimized for multiple data threads.
    }

    public File createTargets(File file) throws Exception {
        setInputFile(file.getPath());

        final String outputIntervalsFilename = Utils.removeExtenstion(file.getPath(), "bam") + "-target.intervals";
        File outputIntervalsFile = new File(outputIntervalsFilename);
        setOutputFile(outputIntervalsFile.getPath());

        executeProgram();
        return outputIntervalsFile;
   }
}
