package com.github.sparkcaller;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import picard.vcf.MergeVcfs;

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

    public static List<File> splitVcf(JavaSparkContext sparkContext, String pathToInputVcf) throws IOException {
        JavaRDD<String> vcfFile = sparkContext.textFile(pathToInputVcf);
        JavaRDD<String> vcfHeader = vcfFile.filter(line -> line.startsWith("#"));
        JavaRDD<String> vcfLines = vcfFile.subtract(vcfHeader);


        // Get the first word in the line, as this maps to the chromosome.
        JavaPairRDD<String, Iterable<String>> vcfChromosomesRDD = vcfLines.groupBy(line -> line.split("\t", 0)[0]);
        List<String> chromosomes = vcfChromosomesRDD.keys().distinct().collect();
        List<File> vcfFilesByChromosome = new ArrayList<>();

        for (String chromosome: chromosomes) {
            JavaRDD<String> chromosomeRDD = getRddByChromosome(vcfChromosomesRDD, chromosome);

            // Sort the variants by position.
            chromosomeRDD = chromosomeRDD.sortBy(line -> Long.parseLong(line.split("\t")[1]), true, 1);

            // Make sure that each file has a header.
            List<String> chromosomeVcfLines = vcfHeader.union(chromosomeRDD).collect();

            File chromosomeOutputFile = new File(Utils.removeExtenstion(pathToInputVcf, "vcf") + "-" + chromosome + ".vcf");
            FileWriter newVcfFile = new FileWriter(chromosomeOutputFile);

            for (String line : chromosomeVcfLines) {
                newVcfFile.write(line + "\n");
            }

            newVcfFile.close();
            vcfFilesByChromosome.add(chromosomeOutputFile);
        }

        return vcfFilesByChromosome;
    }

    public static JavaRDD<String> getRddByChromosome(JavaPairRDD<String, Iterable<String>> chromosomes, String chromosome) {
        // Get the RDD which has the desired 'chromosome' as the key,
        // then get all variants connected to this chromosome and return it as a single RDD.
        return chromosomes.filter(rddChromosome -> rddChromosome._1().equals(chromosome)).values().flatMap(line -> line);
    }

    public static File moveToDir(File fileToCopy, String targetPath) {
        File outputFile = new File(targetPath, fileToCopy.getName());
        fileToCopy.renameTo(outputFile);

        return outputFile;
    }

    public static ArrayList<String> possibleStringToArgs(String maybeString) {
        if (maybeString == null) {
            return null;
        }

        String[] args = maybeString.split("\\s+");
        return new ArrayList<>(Arrays.asList(args));
    }
}
