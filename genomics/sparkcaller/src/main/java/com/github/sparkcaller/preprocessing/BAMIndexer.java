package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.MiscUtils;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.spark.api.java.function.Function;

import java.io.File;

public class BAMIndexer implements Function<File, File> {
    // Create an index (.bai) file for the given BAM file.
    public static void indexBAM(File bamFile) throws Exception {
        final SamReader bamReader = SamReaderFactory.makeDefault()
                .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                .open(bamFile);

        File bamIndexFile = new File(MiscUtils.removeExtenstion(bamFile.getPath(), "bam") + ".bai");
        if (bamIndexFile.exists()) {
            bamIndexFile.delete();
        }

        htsjdk.samtools.BAMIndexer.createIndex(bamReader, bamIndexFile);
    }

    @Override
    public File call(File inputBAM) throws Exception {
        BAMIndexer.indexBAM(inputBAM);
        return inputBAM;
    }
}
