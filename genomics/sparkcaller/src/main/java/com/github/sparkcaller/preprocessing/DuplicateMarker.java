package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.Utils;
import picard.sam.markduplicates.MarkDuplicates;

import java.io.File;
import java.util.ArrayList;

/*
 * Marks all the duplicate reads found in the input BAM file.
 */
public class DuplicateMarker {
    final private ArrayList<String> extraArgs;
    final private String outputFolder;

    public DuplicateMarker(String outputFolder, String extraArgsString) {
        this.extraArgs = Utils.possibleStringToArgs(extraArgsString);
        this.outputFolder = outputFolder;
    }

    public File markDuplicates(File file) throws Exception {
        MarkDuplicates markDuplicates = new MarkDuplicates();

        ArrayList<String> markerArgs = new ArrayList<String>();
        markerArgs.add("INPUT=");
        markerArgs.add(file.getPath());

        String newFileName = Utils.removeExtenstion(file.getPath(), "bam") + "-deduped.bam";
        File outputBamFile = new File(newFileName);

        markerArgs.add("OUTPUT=");
        markerArgs.add(outputBamFile.getPath());

        markerArgs.add("METRICS_FILE=");
        markerArgs.add(file.getPath() + "-metrics.txt");

        if (this.extraArgs != null) {
            markerArgs.addAll(extraArgs);
        }

        markDuplicates.instanceMain(markerArgs.toArray(new String[0]));
        return Utils.moveToDir(outputBamFile, this.outputFolder);
    }
}
