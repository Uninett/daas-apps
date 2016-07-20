package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;

public class RecalibrateVariants extends BaseGATKProgram {

    public RecalibrateVariants(String pathToReference, String extraArgs) {
        super("VariantRecalibrator", extraArgs);
        setReference(pathToReference);
    }

    public void recalibrateVariants(String pathToVcf) throws Exception {
        addArgument("-input", pathToVcf);
        addArgument("-recalFile", Utils.removeExtenstion(pathToVcf, "vcf") + "-recal.recal");
        addArgument("-tranchesFile", Utils.removeExtenstion(pathToVcf, "vcf") + "-tranches.tranches");

        executeProgram();
   }
}
