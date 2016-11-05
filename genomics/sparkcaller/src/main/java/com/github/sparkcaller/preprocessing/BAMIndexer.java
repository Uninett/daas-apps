package com.github.sparkcaller.preprocessing;

import com.github.sparkcaller.utils.MiscUtils;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;

public class BAMIndexer implements Function<Tuple2<File, File>, Tuple2<File, File>> {
    // Create an index (.bai) file for the given BAM file.
    public static File indexBAM(File bamFile) throws Exception {
        final SamReader bamReader = SamReaderFactory.makeDefault()
                .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                .open(bamFile);

        File bamIndexFile = new File(MiscUtils.removeExtenstion(bamFile.getPath(), "bam") + ".bai");
        if (bamIndexFile.exists()) {
            bamIndexFile.delete();
        }

        htsjdk.samtools.BAMIndexer.createIndex(bamReader, bamIndexFile);

        return bamFile;
    }

    @Override
    public Tuple2<File, File> call(Tuple2<File, File> inputOutputTuple) throws Exception {
        File inputBAM = inputOutputTuple._2;

        BAMIndexer.indexBAM(inputBAM);
        return inputOutputTuple;
    }
}
