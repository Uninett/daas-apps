package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;

import java.io.File;

public class HaplotypeCaller extends BaseGATKProgram implements Function<File, File> {

    public HaplotypeCaller(String pathToReference, String extraArgsString) {
        super("HaplotypeCaller", extraArgsString);
        setReference(pathToReference);
    }

    public File call(File bamFile) throws Exception {
        String outputFilename = Utils.removeExtenstion(bamFile.getPath(), "bam")  + ".vcf";
        setInputFile(bamFile.getAbsolutePath());
        setOutputFile(outputFilename);

        executeProgram();
        return bamFile;
    }
}
