package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.MiscUtils;
import org.apache.spark.api.java.function.Function;
import picard.sam.SortSam;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class SAMToSortedBAM implements Function<Tuple2<File, File>, Tuple2<File, File>> {
    public Tuple2<File, File> call(Tuple2<File, File> inputOutputSAM) throws Exception {
        File inputSAM = inputOutputSAM._2;

        String newFileName = MiscUtils.removeExtenstion(inputSAM.getName(), "sam") + ".bam";
        File outputSamFile = new File(newFileName);

        ArrayList<String> sorterArgs = new ArrayList<String>();
        sorterArgs.add("INPUT=");
        sorterArgs.add(inputSAM.getPath());

        sorterArgs.add("OUTPUT=");
        sorterArgs.add(outputSamFile.getPath());

        sorterArgs.add("SORT_ORDER=coordinate");
        SortSam samSorter = new SortSam();
        samSorter.instanceMain(sorterArgs.toArray(new String[0]));

        return new Tuple2<>(inputSAM, outputSamFile);
    }
}
