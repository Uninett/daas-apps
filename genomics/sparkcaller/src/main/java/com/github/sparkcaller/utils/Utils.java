package com.github.sparkcaller.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import scala.Tuple2;

import java.io.*;
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

    public static List<Tuple2<String, File>> moveFilesToDir(List<Tuple2<String, File>> files, String outputFolder) throws IOException {
        List<Tuple2<String, File>> outputFiles = new ArrayList<>();
        for (Tuple2<String, File> tuple : files) {
            File newFile = Utils.moveToDir(tuple._2, outputFolder);
            outputFiles.add(new Tuple2<>(tuple._1, newFile));
        }

        return outputFiles;
    }

    public static File moveToDir(File fileToCopy, String targetPath) throws IOException {
        File outputFile = new File(targetPath, fileToCopy.getName());

        if (!outputFile.exists()) {
            FileUtils.moveFile(fileToCopy, outputFile);
        }

        return outputFile;
    }

    public static ArrayList<String> possibleStringToArgs(String maybeString) {
        if (maybeString == null || maybeString.isEmpty()) {
            return null;
        }

        String[] args = maybeString.split("\\s+");
        return new ArrayList<>(Arrays.asList(args));
    }
}
