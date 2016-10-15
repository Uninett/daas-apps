package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.MiscUtils;
import picard.sam.markduplicates.MarkDuplicates;

import java.io.File;
import java.util.ArrayList;

/*
 * Marks all the duplicate reads found in the input BAM file.
 */
public class DuplicateMarker {

    public static File markDuplicates(File file, String outputFolder, String extraArgsString) throws Exception {
        MarkDuplicates markDuplicates = new MarkDuplicates();
        ArrayList<String> extraArgs = MiscUtils.possibleStringToArgs(extraArgsString);

        ArrayList<String> markerArgs = new ArrayList<String>();
        markerArgs.add("INPUT=");
        markerArgs.add(file.getPath());

        String newFileName = MiscUtils.removeExtenstion(file.getPath(), "bam") + "-deduped.bam";
        File outputBamFile = new File(newFileName);

        markerArgs.add("CREATE_INDEX=");
        markerArgs.add("true");

        markerArgs.add("OUTPUT=");
        markerArgs.add(outputBamFile.getPath());

        markerArgs.add("METRICS_FILE=");
        markerArgs.add(file.getPath() + "-metrics.txt");

        if (extraArgs != null) {
            markerArgs.addAll(extraArgs);
        }

        markDuplicates.instanceMain(markerArgs.toArray(new String[0]));
        return MiscUtils.moveToDir(outputBamFile, outputFolder);
    }
}
