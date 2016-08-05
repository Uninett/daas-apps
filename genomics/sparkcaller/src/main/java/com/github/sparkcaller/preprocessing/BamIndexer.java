package com.github.sparkcaller.preprocessing;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.File;

public class BamIndexer implements Function<File, File> {
    // Create an index (.bai) file for the given BAM file.
    public static void indexBam(File bamFile) throws Exception {
        final SamReader bamReader = SamReaderFactory.makeDefault()
                                              .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                                              .open(bamFile);
        BAMIndexer.createIndex(bamReader, new File(bamFile.getPath() + ".bai"));
    }

    @Override
    public File call(File inputBAM) throws Exception {
        BamIndexer.indexBam(inputBAM);
        return inputBAM;
    }
}
