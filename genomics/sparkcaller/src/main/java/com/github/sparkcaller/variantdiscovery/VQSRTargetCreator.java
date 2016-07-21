package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import scala.Tuple2;

import java.io.File;

public class VQSRTargetCreator extends BaseGATKProgram {

    public VQSRTargetCreator(String pathToReference, String extraArgs, String coresPerNode) {
        super("VariantRecalibrator", extraArgs);
        setReference(pathToReference);
        addArgument("-nt", coresPerNode);

    }

    private String constructRecalibrateFilename(String inputFilename, String mode, String extension) {
        return String.format("%s-%s.%s", Utils.removeExtenstion(inputFilename, "vcf"), mode, extension);
    }

    public Tuple2<File, File> createTargets(File vcfInput, String mode) throws Exception {
        changeArgument("-input", vcfInput.getPath());

        // Either SNP or INDEL
        changeArgument("-mode", mode);

        File recalOutput = new File(constructRecalibrateFilename(vcfInput.getPath(), mode, "recal"));
        File tranchesOutput = new File(constructRecalibrateFilename(vcfInput.getPath(), mode, "tranches"));
        changeArgument("-recalFile", recalOutput.getPath());
        changeArgument("-tranchesFile", tranchesOutput.getPath());

        executeProgram();
        return new Tuple2<>(recalOutput, tranchesOutput);
    }
}
