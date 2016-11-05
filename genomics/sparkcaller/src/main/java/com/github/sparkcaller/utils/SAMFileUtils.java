package com.github.sparkcaller.utils;

import picard.sam.AddOrReplaceReadGroups;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SAMFileUtils {

    public static File mergeBAMFiles(List<File> samFiles, String outputFileName, String coresPerNode) throws Exception {
        File outputFile;

        if (samFiles.size() > 1) {
            // Make sure that the output file has a proper file extension.
            if (!outputFileName.endsWith("bam")) {
                outputFileName = outputFileName + ".bam";
            }

            outputFile = new File(outputFileName);

            ArrayList<String> args = new ArrayList<>();
            args.add("merge");
            args.add("-@");
            args.add(coresPerNode);
            args.add(outputFile.getPath());

            for (File samFile : samFiles) {
                args.add(samFile.getPath());
            }

            MiscUtils.executeResourceBinary("samtools", args);
        } else if (samFiles.size() != 0) {
            outputFile = samFiles.get(0);
        } else {
            System.err.println("No files was given!");
            System.exit(1);
            return null;
        }

        return outputFile;
    }

    public static File addOrReplaceRG(File inputFile, String args) {
        AddOrReplaceReadGroups addOrReplaceReadGroupsEngine = new AddOrReplaceReadGroups();
        File outputFile = new File(MiscUtils.removeExtenstion(inputFile.getName(), "bam" )+ "-rg" + ".bam");
        ArrayList<String> extraArgs = MiscUtils.possibleStringToArgs(args);


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
