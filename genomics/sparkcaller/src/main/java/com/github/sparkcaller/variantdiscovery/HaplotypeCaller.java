package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.utils.BaseGATKProgram;
import com.github.sparkcaller.utils.MiscUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class HaplotypeCaller extends BaseGATKProgram implements Function<Tuple2<String, Tuple2<File, File>>, Tuple2<File, File>> {

    public HaplotypeCaller(String pathToReference, String extraArgsString, String availableCoresPerNode) {
        super("HaplotypeCaller", extraArgsString);
        setReference(pathToReference);
        setThreads(availableCoresPerNode);
    }

    public Tuple2<File, File> call(Tuple2<String, Tuple2<File, File>> contigTuple) throws Exception {
        String contig = contigTuple._1;
        Tuple2<File, File> inputOutputFiles = contigTuple._2;
        File inputBam = inputOutputFiles._2;

        String outputFilename = MiscUtils.removeExtenstion(inputBam.getPath(), "bam")  + ".vcf";
        setInterval(contig);
        setInputFile(inputBam.getPath());
        setOutputFile(outputFilename);

        executeProgram();
        File outputFile = new File(outputFilename);
        return new Tuple2<>(inputOutputFiles._1, outputFile);
    }
}
