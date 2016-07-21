package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;

import java.io.File;
import java.util.List;

public class GenotypeGVCF extends BaseGATKProgram {

    public GenotypeGVCF(String pathToReference, String extraArgs, String availableCoresPerNode) {
        super("GenotypeGVCFs", extraArgs);
        setReference(pathToReference);
        addArgument("-nt", availableCoresPerNode);
    }

    public File performJointGenotyping(List<File> vcfFilenames, String outputFilename) throws Exception {
        for (File vcfFile : vcfFilenames) {
            addArgument("--variant", vcfFile.getPath());
        }

        setOutputFile(outputFilename);
        executeProgram();

        return new File(outputFilename);
    }
}
