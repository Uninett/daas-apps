package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.MiscUtils;
import org.apache.spark.api.java.function.Function;
import picard.sam.markduplicates.MarkDuplicates;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

/*
 * Marks all the duplicate reads found in the input BAM file using Picard.
 */
public class DuplicateMarker implements Function<File, Tuple2<File, File>> {
    final private String extraArgs;

    public DuplicateMarker(String extraArgs) {
        this.extraArgs = extraArgs;
    }

    public static File markDuplicates(File file, String extraArgsString) throws Exception {
        MarkDuplicates markDuplicates = new MarkDuplicates();
        ArrayList<String> extraArgs = MiscUtils.possibleStringToArgs(extraArgsString);

        ArrayList<String> markerArgs = new ArrayList<String>();
        markerArgs.add("INPUT=");
        markerArgs.add(file.getPath());

        String newFileName = MiscUtils.removeExtenstion(file.getName(), "bam") + "-deduped.bam";
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
        return outputBamFile;
    }

    @Override
    public Tuple2<File, File> call(File inputBAM) throws Exception {
        File outputBAM = DuplicateMarker.markDuplicates(inputBAM, this.extraArgs);
        return new Tuple2<>(inputBAM, outputBAM);
    }
}
