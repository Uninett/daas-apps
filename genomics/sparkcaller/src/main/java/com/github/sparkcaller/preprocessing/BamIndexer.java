package com.github.sparkcaller.preprocessing;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.spark.api.java.function.Function;

import java.io.File;

public class BamIndexer implements Function<File, File> {
    // Create an index (.bai) file for the given BAM file.
    public File call(File bamFile) throws Exception {
        final SamReader bamReader = SamReaderFactory.makeDefault()
                                              .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                                              .open(bamFile);
        BAMIndexer.createIndex(bamReader, new File(bamFile.getPath() + ".bai"));

        return bamFile;
    }
}
