package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.BaseGATKProgram;
import com.github.sparkcaller.Utils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

public class IndelTargetCreator extends BaseGATKProgram implements Function<File, Tuple2<File, File>> {
    public IndelTargetCreator(String pathToReference, String extraArgsString) {
        super("RealignerTargetCreator", extraArgsString);
        setReference(pathToReference);
    }

    public Tuple2<File, File> call(File file) throws Exception {
        setInputFile(file.getPath());

        final String outputIntervalsFilename = Utils.removeExtenstion(file.getPath(), "bam") + "-target.intervals";
        File outputIntervalsFile = new File(outputIntervalsFilename);
        setOutputFile(outputIntervalsFile.getPath());

        executeProgram();
        return new Tuple2<File, File>(file, outputIntervalsFile);
   }
}
