package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.BaseGATKProgram;
import com.github.sparkcaller.utils.MiscUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;

/*
 * Realign the indels found by the RealignerTargetCreator.
 *
 * See:
 * https://www.broadinstitute.org/gatk/guide/article?id=38
 *
 * For more information.
 */
public class RealignIndels extends BaseGATKProgram implements Function<Tuple2<String, Tuple2<File, File>>, Tuple2<File, File>> {

    private final HashMap<String, File> indelTargetsMapper;

    public RealignIndels(String pathToReference, HashMap<String, File> indelTargetsMapper, String extraArgs) {
        super("IndelRealigner", extraArgs);
        setReference(pathToReference);
        this.indelTargetsMapper = indelTargetsMapper;
}

    public Tuple2<File, File> call(Tuple2<String, Tuple2<File, File>> contigTuple) throws Exception {
        String contig = contigTuple._1;
        Tuple2<File, File> inputOutputTuple = contigTuple._2;

        File inputBam = inputOutputTuple._2;

        File indelTargets = this.indelTargetsMapper.get(inputOutputTuple._1.getParentFile().getParent());
        changeArgument("-targetIntervals", indelTargets.getPath());

        setInterval(contig);
        setInputFile(inputBam.getPath());

        final String newFileName = MiscUtils.removeExtenstion(inputBam.getName(), "bam") + "-realigned.bam";

        File outputBamFile = new File(newFileName);
        setOutputFile(outputBamFile.getPath());

        executeProgram();
        return new Tuple2<>(inputOutputTuple._1, outputBamFile);
    }
}
