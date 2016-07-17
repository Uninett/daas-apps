package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class BQSRTargetGenerator extends BaseGATKProgram implements Function<File, Tuple2<File, File>> {
    public BQSRTargetGenerator(String pathToReference, String knownSites, String extraArgs) {
        super("BaseRecalibrator", extraArgs);
        setReference(pathToReference);
        addArgument("-knownSites", knownSites);
    }

    public Tuple2<File, File> call(File file) throws Exception {
        setInputFile(file.getPath());
        String outputTableFilename = Utils.removeExtenstion(file.getPath(), "bam") + "-recal_data.table";
        File outputTable = new File(outputTableFilename);

        setOutputFile(outputTable.getPath());

        executeProgram();
        return new Tuple2<File, File>(file, outputTable);
    }
}
