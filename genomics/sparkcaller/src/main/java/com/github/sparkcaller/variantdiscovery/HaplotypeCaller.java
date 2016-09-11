package com.github.sparkcaller.variantdiscovery;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class HaplotypeCaller extends BaseGATKProgram implements Function<Tuple2<String, File>, File> {

    public HaplotypeCaller(String pathToReference, String extraArgsString, String availableCoresPerNode) {
        super("HaplotypeCaller", extraArgsString);
        setReference(pathToReference);
        setThreads(availableCoresPerNode);
    }

    public File call(Tuple2<String, File> contigTuple) throws Exception {
        String contig = contigTuple._1;
        File inputBam = contigTuple._2;

        String outputFilename = Utils.removeExtenstion(inputBam.getPath(), "bam")  + ".vcf";
        setInterval(contig);
        setInputFile(inputBam.getAbsolutePath());
        setOutputFile(outputFilename);

        executeProgram();
        return new File(outputFilename);
    }
}
