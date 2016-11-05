package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.BaseGATKProgram;
import com.github.sparkcaller.utils.MiscUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

/*
 * Recalibrate the base quality scores inorder to get more accurate scores.
 *
 * See:
 * https://www.broadinstitute.org/gatk/documentation/tooldocs/org_broadinstitute_gatk_tools_walkers_bqsr_BaseRecalibrator.php
 *
 * For more information.
 *
 */
public class BQSRTargetGenerator extends BaseGATKProgram implements Function<File, Tuple2<File, File>> {

    public BQSRTargetGenerator(String pathToReference, String knownSites, String extraArgs, String coresPerNode) {
        super("BaseRecalibrator", extraArgs);
        setReference(pathToReference);
        addArgument("-knownSites", knownSites);
        setThreads(coresPerNode);
    }

    public File generateTargets(File file) throws Exception {
        setInputFile(file.getPath());
        String outputTableFilename = MiscUtils.removeExtenstion(file.getName(), "bam") + "-recal_data.table";
        File outputTable = new File(outputTableFilename);

        setOutputFile(outputTable.getPath());

        executeProgram();
        return outputTable;
    }

    @Override
    public Tuple2<File, File> call(File inputBAM) throws Exception {
        File bqsrTargets = this.generateTargets(inputBAM);
        return new Tuple2<>(inputBAM, bqsrTargets);
    }
}
