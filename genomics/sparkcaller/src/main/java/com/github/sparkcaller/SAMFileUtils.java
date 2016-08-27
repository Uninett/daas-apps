package com.github.sparkcaller;

import htsjdk.samtools.*;
import picard.sam.MergeSamFiles;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SAMFileUtils {

    public static File mergeBAMFiles(List<File> samFiles, String outputFileName) {
        MergeSamFiles samMerger = new MergeSamFiles();
        samMerger.USE_THREADING = true;
        samMerger.ASSUME_SORTED = true;
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

    public static List<Tuple2<String, File>> splitBAMByChromosome(File bamFile, String outputFolder) throws IOException {
        SamReader samFile = SamReaderFactory.makeDefault().open(bamFile);
        SAMFileHeader samFileHeader = samFile.getFileHeader();
        HashMap<String, SAMFileWriter> contigMapper = new HashMap<>();
        SAMFileWriterFactory samWriterFactory =  new SAMFileWriterFactory();

        SAMRecordIterator samIterator = samFile.iterator();
        String prevContig = "NONE";
        SAMFileWriter currWriter = null;

        List<Tuple2<String, File>> outputFiles = new ArrayList<>();
        while(samIterator.hasNext()) {
            SAMRecord record = samIterator.next();
            String currContig = record.getContig();

            if (currContig == null) {
                currContig = "unmapped";
            }

            // When the input is sorted, do not perform a loop up for every record.
            if (!prevContig.equals(currContig)) {
                currWriter = contigMapper.get(currContig);
            }

            // Create a new writer if a new contig was found.
            if (currWriter == null) {
                String newSAMFilename = Utils.removeExtenstion(bamFile.getName(), "bam") + "-" + currContig + ".bam";
                File newSAMFile = new File(newSAMFilename);
                SAMFileWriter newWriter = samWriterFactory.makeSAMOrBAMWriter(samFileHeader, true, newSAMFile);
                newWriter.addAlignment(record);
                contigMapper.put(currContig, newWriter);

                currWriter = newWriter;
                outputFiles.add(new Tuple2<>(currContig, newSAMFile));
            } else {
                currWriter.addAlignment(record);
            }

            prevContig = currContig;
        }
        contigMapper.values().forEach(SAMFileWriter::close);

        return Utils.moveFilesToDir(outputFiles, outputFolder);
    }
}
