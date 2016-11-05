package com.github.sparkcaller.utils;

import org.apache.spark.api.java.function.Function;
import picard.vcf.MergeVcfs;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class VCFMerger implements Function<Tuple2<String, Iterable<File>>, Tuple2<File, File>> {

    final private String outputName;

    public VCFMerger(String outputName) {
        this.outputName = outputName;
    }

    /*
    Merges all the VCF files specified in the list 'vcfFiles' and returns the filename of the output file.
    */
    public static File mergeVCFFiles(List<File> vcfFiles, String outputFileName) {
        MergeVcfs mergeEngine = new MergeVcfs();
        mergeEngine.INPUT = vcfFiles;

        if (!outputFileName.endsWith("vcf")) {
            outputFileName = outputFileName + ".vcf";
        }


        // Only pass the output file as an argument,
        // as it is more efficient to set the input files directly in the object.
        String outputArgs[] = {"O=" + outputFileName};

        mergeEngine.instanceMain(outputArgs);
        return new File(outputFileName);
    }

    @Override
    public Tuple2<File, File> call(Tuple2<String, Iterable<File>> stringIterableTuple2) throws Exception {
        List<File> filesToMerge = new ArrayList<>();
        for (File vcfFile : stringIterableTuple2._2) {
            filesToMerge.add(vcfFile);
        }

        File inputFile = filesToMerge.get(0);
        String baseOutputName = inputFile.getName();

        String[] outputFileNameParts = baseOutputName.split("-");
        String outputFileName = outputFileNameParts[0];

        for (int i = 1; i < outputFileNameParts.length-2; i++) {
            outputFileName += "-" + outputFileNameParts[i];
        }

        outputFileName += "-" + this.outputName;

        File outputFile = VCFMerger.mergeVCFFiles(filesToMerge, outputFileName);
        return new Tuple2<>(inputFile, outputFile);
    }
}
