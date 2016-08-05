package com.github.sparkcaller;

import htsjdk.samtools.*;
import picard.sam.MergeSamFiles;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SAMFileUtils {

    public static File mergeBAMFiles(List<File> samFiles, String outputFileName) {
        MergeSamFiles samMerger = new MergeSamFiles();
        ArrayList<String> args = new ArrayList<>();

        for (File samFile : samFiles) {
            args.add("I=");
            args.add(samFile.getPath());
        }

        args.add("O=");
        args.add(outputFileName);

        samMerger.instanceMain(args.toArray(new String[0]));

        return new File(outputFileName);
    }

    public static int getSAMNumberOfLines(SamReader samReader) {
        SAMRecordIterator samIterator = samReader.iterator();

        int samRecordSize = 0;
        // Get the size of the SAM record.
        while(samIterator.hasNext()) {
            SAMRecord record = samIterator.next();
            samRecordSize++;
        }
        samIterator.close();

        return samRecordSize;
    }

    public static List<File> splitBAMFile(File bamFile, int numSplits) throws IOException {
        List<File> outputFiles = new ArrayList<>();

        SamReader samFile = SamReaderFactory.makeDefault().open(bamFile);

        int SAMNumRecords = getSAMNumberOfLines(samFile);

        SAMFileWriterFactory samWriterFactory =  new SAMFileWriterFactory();
        SAMRecordIterator samIterator = samFile.iterator();

        // We rather want to have fewer records per file and more files, as this makes it easier to utilize all nodes.
        int recordsPerFile = SAMNumRecords / numSplits;

        int currNumRecords = 0;
        int currFileNum = 0;

        SAMFileHeader samHeader = samFile.getFileHeader();

        String splitBaseFilename = Utils.removeExtenstion(bamFile.getPath(), "bam");
        Tuple2<SAMFileWriter, File> currSplitTuple = getSplitSAMWriter(splitBaseFilename, currFileNum, true,
                                                                       samHeader, samWriterFactory);
        SAMFileWriter currSplitSAMWriter = currSplitTuple._1;
        outputFiles.add(currSplitTuple._2);

        while(samIterator.hasNext()) {
            if (currNumRecords == recordsPerFile) {
                currSplitSAMWriter.close();

                currNumRecords = 0;
                currFileNum++;
                currSplitTuple = getSplitSAMWriter(splitBaseFilename, currFileNum, true, samHeader, samWriterFactory);

                currSplitSAMWriter = currSplitTuple._1;
                outputFiles.add(currSplitTuple._2);
            }

            currSplitSAMWriter.addAlignment(samIterator.next());
            currNumRecords++;
        }

        currSplitSAMWriter.close();
        samIterator.close();
        samFile.close();

        return outputFiles;
    }

    public static Tuple2<SAMFileWriter, File> getSplitSAMWriter(String baseFilename, int id, boolean isSorted,
                                                                SAMFileHeader SAMheader,
                                                                SAMFileWriterFactory samFileWriterFactory) {

        File newSplitFile = new File(baseFilename + "-split-" + Integer.toString(id) + ".bam");
        SAMFileWriter currSAMWriter = samFileWriterFactory.makeSAMOrBAMWriter(SAMheader, isSorted, newSplitFile);
        return new Tuple2<>(currSAMWriter, newSplitFile);

    }
}
