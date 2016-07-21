package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;

import java.io.File;

public class RecalibrateVariants extends BaseGATKProgram {

    public RecalibrateVariants(String pathToReference, String extraArgs, String coresPerNode) {
        super("VariantRecalibrator", extraArgs);
        setReference(pathToReference);
        addArgument("-nt", coresPerNode);
    }

    public File performRecalibration(File vcfInput) throws Exception {
        addArgument("-input", vcfInput.getPath());

        File recalOutput = new File(Utils.removeExtenstion(vcfInput.getPath(), "vcf") + "-recal.recal");
        addArgument("-recalFile", recalOutput.getPath());
        addArgument("-tranchesFile", Utils.removeExtenstion(vcfInput.getPath(), "vcf") + "-tranches.tranches");

        executeProgram();
        return recalOutput;
    }
}
