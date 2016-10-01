package com.github.sparkcaller.utils;

import htsjdk.samtools.*;
import picard.sam.AddOrReplaceReadGroups;
import picard.sam.MergeSamFiles;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SAMFileUtils {

    public static File mergeBAMFiles(List<File> samFiles, String outputPath, String outputFileName) throws IOException {
        File outputFile;
        if (samFiles.size() > 1) {

            // Make sure that the output file has a proper file extension.
            if (!outputFileName.endsWith("bam")) {
                outputFileName = outputFileName + ".bam";
            }

            outputFile = new File(outputPath, outputFileName);
            MergeSamFiles samMerger = new MergeSamFiles();
            samMerger.USE_THREADING = true;
            samMerger.ASSUME_SORTED = true;
            samMerger.CREATE_INDEX = true;
            ArrayList<String> args = new ArrayList<>();

            for (File samFile : samFiles) {
                args.add("I=");
                args.add(samFile.getPath());
            }

            args.add("O=");
            args.add(outputFile.getPath());

            samMerger.instanceMain(args.toArray(new String[0]));
        } else if (samFiles.size() != 0) {
            outputFile = samFiles.get(0);
        } else {
            System.err.println("No files was given!");
            System.exit(1);
            return null;
        }

        return outputFile;

    }

    public static List<Tuple2<String, File>> splitBAMByChromosome(File bamFile, String outputFolder) throws IOException {
        SamReader samFile = SamReaderFactory.makeDefault().open(bamFile);
        SAMFileHeader samFileHeader = samFile.getFileHeader();

        HashMap<String, SAMFileWriter> contigMapper = new HashMap<>();
        SAMFileWriterFactory samWriterFactory =  new SAMFileWriterFactory();

        String prevContig = "NONE";
        SAMFileWriter currWriter = null;

        List<Tuple2<String, File>> outputFiles = new ArrayList<>();
        for (final SAMRecord record : samFile) {
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

    public static File addOrReplaceRG(File inputFile, String outputFolder, String args) {
        AddOrReplaceReadGroups addOrReplaceReadGroupsEngine = new AddOrReplaceReadGroups();
        File outputFile = new File(outputFolder, Utils.removeExtenstion(inputFile.getName(), "bam" )+ "-rg" + ".bam");
        ArrayList<String> extraArgs = Utils.possibleStringToArgs(args);


        ArrayList<String> argsList = new ArrayList<>();
        argsList.add("CREATE_INDEX=true");
        argsList.add("INPUT=" + inputFile.getPath());
        argsList.add("OUTPUT=" + outputFile.getPath());

        if (extraArgs != null) {
            argsList.addAll(extraArgs);
        }

        addOrReplaceReadGroupsEngine.instanceMain(argsList.toArray(new String[0]));
        return outputFile;
    }
}
