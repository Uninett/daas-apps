package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;

import java.io.File;

/*
 * Use the targets generated by the BQSRTargetGenerator to perform BQSR on the input BAM file.
 * Writes a BAM file with recalibrated bases.
 *
 * See:
 * https://www.broadinstitute.org/gatk/documentation/tooldocs/org_broadinstitute_gatk_tools_walkers_readutils_PrintReads.php
 *
 * For more information.
 *
 */
public class BQSR extends BaseGATKProgram implements Function<File, File> {
    public BQSR(String pathToReference, String bqsrTablePath, String extraArgsString, String coresPerNode) {
        super("PrintReads", extraArgsString);
        setReference(pathToReference);
        addArgument("-BQSR", bqsrTablePath);
        setThreads(coresPerNode);
    }

    @Override
    public File call(File bamFile) throws Exception {
        setInputFile(bamFile.getPath());

        String outputBamFilename = Utils.removeExtenstion(bamFile.getPath(), "bam") + "-bqsr.bam";
        File outputBam = new File(outputBamFilename);

        setOutputFile(outputBam.getPath());

        executeProgram();
        return outputBam;
    }
}
