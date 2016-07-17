package com.github.sparkcaller;

import picard.vcf.MergeVcfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Utils {
    /*
    Get all the files in the folder 'pathToFolder' ending with 'fileExtension', and return them as an ArrayList.
    */
    public static ArrayList<File> getFilesInFolder(String pathToFolder, String fileExtension) {
        File folder = new File(pathToFolder);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles != null) {
            ArrayList<File> bamFiles = new ArrayList<File>();

            for (File file : listOfFiles) {
                if (file.isFile() && file.getName().endsWith(fileExtension)) {
                    bamFiles.add(file);
                }
            }
            return bamFiles;
        }

        System.err.println(String.format("The folder: %s is either empty or was not found!", folder.getName()));
        return null;
    }

    public static String removeExtenstion(String filepath, String ext) {
        return filepath.substring(0, filepath.length() - (ext.length()+1));
    }

    /*
    Merges all the VCF files specified in the list 'vcfFiles' and returns the filename of the output file.
    */
    public static String mergeVCFFiles(List<File> vcfFiles, String outputFileName) {
        MergeVcfs mergeEngine = new MergeVcfs();
        mergeEngine.INPUT = vcfFiles;

        // Only pass the output file as an argument,
        // as it is more efficient to set the input files directly in the object.
        String outputArgs[] = {"O=" + outputFileName};

        mergeEngine.instanceMain(outputArgs);
        return outputFileName;
    }

    public static Properties loadConfigFile(String configFilepath) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(configFilepath);
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return prop;
    }

    public static ArrayList<String> possibleStringToArgs(String maybeString) {
        if (maybeString == null) {
            return null;
        }

        String[] args = maybeString.split("\\s+");
        return new ArrayList<String>(Arrays.asList(args));
    }
}
