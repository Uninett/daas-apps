package com.github.sparkcaller.preprocessing;

import org.apache.spark.api.java.function.Function;
import picard.sam.SortSam;

import java.io.File;
import java.util.ArrayList;

public class SamToSortedBam implements Function<File, File> {
    public File call(File file) throws Exception {
        final String ext = "sam";
        String newFileName = file.getPath().substring(0, file.getPath().length() - ext.length()) + "bam";
        File outputSamFile = new File(newFileName);

        ArrayList<String> sorterArgs = new ArrayList<String>();
        sorterArgs.add("INPUT=");
        sorterArgs.add(file.getPath());

        sorterArgs.add("OUTPUT=");
        sorterArgs.add(outputSamFile.getPath());

        sorterArgs.add("SORT_ORDER=coordinate");
        SortSam samSorter = new SortSam();
        samSorter.instanceMain(sorterArgs.toArray(new String[0]));

        return outputSamFile;
    }
}
