package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.SAMFileUtils;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class AddOrReplaceRGs implements Function<File, Tuple2<File, File>> {
    final private String extraArgs;

    public AddOrReplaceRGs(String extraArgs) {
        this.extraArgs = extraArgs;
    }
    @Override
    public Tuple2<File, File> call(File inputFile) throws Exception {
        File outputWithRG = SAMFileUtils.addOrReplaceRG(inputFile, this.extraArgs);

        return new Tuple2<>(inputFile, outputWithRG);
    }
}
