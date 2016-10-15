package com.github.sparkcaller.utils;

import org.apache.spark.api.java.function.Function;
import java.io.File;

public class FileMover implements Function<File, File> {
    String targetPath;

    public FileMover(String targetPath) {
        this.targetPath = targetPath;
    }

    @Override
    public File call(File file) throws Exception {
        return MiscUtils.moveToDir(file, this.targetPath);
    }
}
