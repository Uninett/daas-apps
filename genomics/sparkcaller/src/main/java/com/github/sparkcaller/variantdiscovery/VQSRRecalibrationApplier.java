package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import scala.Tuple2;

import java.io.File;

public class VQSRRecalibrationApplier extends BaseGATKProgram {
    public VQSRRecalibrationApplier(String pathToReference, String extraArgs, String coresPerNode) {
        super("ApplyRecalibration", extraArgs);
        setReference(pathToReference);
        addArgument("-nt", coresPerNode);
    }

    public static String constructRecalFilename(String vcfToRecalibrate, String mode) {
        return Utils.removeExtenstion(vcfToRecalibrate, "vcf") + "-recalib-" + mode + ".vcf";
    }

    public File applyRecalibration(File vcfToRecalibrate, Tuple2<File, File> recalibrationFiles, String mode) {
        File recalibrationFile = recalibrationFiles._1;
        File tranchesFile = recalibrationFiles._2;

        changeArgument("-mode", mode);
        changeArgument("-input", vcfToRecalibrate.getPath());

        changeArgument("-tranchesFile", tranchesFile.getPath());
        changeArgument("-recalFile", recalibrationFile.getPath());

        File recalibratedOutput = new File(constructRecalFilename(vcfToRecalibrate.getPath(), mode));
        setOutputFile(recalibratedOutput.getPath());
        return recalibratedOutput;
    }
}
