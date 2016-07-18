package com.github.sparkcaller;

import com.github.sparkcaller.preprocessing.*;
import com.github.sparkcaller.variantdiscovery.HaplotypeCaller;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;

import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SparkCaller {
    final private JavaSparkContext sparkContext;
    final private Logger log;

    public SparkCaller(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.log = Logger.getLogger(this.getClass());
    }

    /* Performs the preprocessing stage of the GATK pipeline.
     * This is performed in a simple scatter-gather manner.
     * See the following link for details: https://www.broadinstitute.org/gatk/guide/bp_step.php?p=1
     *
     * @param pathToSAMFiles   the path to the folder containing the SAM files created by the aligner.
     *                         Keep in mind that this has to be reachable by all the nodes in the Spark cluster.
     *
     * @param pathToReference  the path to the FASTA reference which was used in the alignment step.
     *                         Keep in mind that this has to be reachable by all the nodes in the Spark cluster.
     *
     * @param knownSites       the path to the database of known polymorphic sites to mask out.
     *                         Keep in mind that this has to be reachable by all the nodes in the Spark cluster.
     *
     * @param toolsExtraArgs   a properties object containing the extra args to pass to each tool.
     *
    */
    public JavaRDD<File> preprocessSAMFiles(String pathToSAMFiles, String pathToReference,
                                            String knownSites, Properties toolsExtraArguments) {
        this.log.info("Preprocessing SAM files!");
        // Get all the SAM files generated by the aligner
        ArrayList<File> samFiles = Utils.getFilesInFolder(pathToSAMFiles, "sam");

        if (samFiles != null) {
            this.log.info("Distributing the SAM files to the nodes...");
            JavaRDD<File> samFilesRDD = sparkContext.parallelize(samFiles);

            this.log.info("Converting the SAM files to sorted BAM files...");
            JavaRDD<File> bamFilesRDD = samFilesRDD.map(new SamToSortedBam());

            this.log.info("Marking duplicates...");
            bamFilesRDD = bamFilesRDD.map(new DuplicateMarker(toolsExtraArguments.getProperty("MarkDuplicates")));

            // Create an index, since this is required by the indel realigner
            this.log.info("Creating BAM indexes...");
            bamFilesRDD = bamFilesRDD.map(new BamIndexer());

            this.log.info("Creating indel targets...");
            JavaRDD<Tuple2<File, File>> indelTargetsRDD = bamFilesRDD.map(new IndelTargetCreator(pathToReference,
                                                                          toolsExtraArguments.getProperty("RealignerTargetCreator")));

            this.log.info("Realigning indels...");
            bamFilesRDD = indelTargetsRDD.map(new RealignIndels(pathToReference));

            this.log.info("Creating targets on which to perform BQSR...");
            JavaRDD<Tuple2<File, File>> bqsrTables = bamFilesRDD.map(new BQSRTargetGenerator(pathToReference, knownSites,
                                                                     toolsExtraArguments.getProperty("BaseRecalibrator")));

            this.log.info("Performing BQSR...");
            bamFilesRDD = bqsrTables.map(new BQSR(pathToReference, toolsExtraArguments.getProperty("PrintReads")));

            this.log.info("Preprocessing finished!");
            return bamFilesRDD;
        }

        return null;

    }

    /* Performs the variant discovery stage of the GATK pipeline.
     * See the following link for details: https://www.broadinstitute.org/gatk/guide/bp_step.php?p=2
     *
     * @param preprocessedBAMFiles   a spark RDD containing the File object for each preprocessed BAM file.
     *
     * @param pathToReference  the path to the FASTA reference which was used in the alignment step.
     *                         Keep in mind that this has to be reachable by all the nodes in the Spark cluster.
     *
     * @param toolsExtraArgs   a properties object containing the extra args to pass to each tool.
     *
    */
    public JavaRDD<File> discoverVariants(JavaRDD<File> preprocessedBAMFiles, String pathToReference,
                                          Properties toolsExtraArgs) {
        log.info("Starting variant discovery!");
        log.info("Running HaplotypeCaller...");
        JavaRDD<File> variantsVCFFiles = preprocessedBAMFiles.map(new HaplotypeCaller(pathToReference,
                                                                  toolsExtraArgs.getProperty("HaplotypeCaller")));

        return variantsVCFFiles;
    }

    public List<File> runPipeline(String pathToSAMFiles, String pathToReference,
                                  String knownSites, Properties toolsExtraArguments) {

        JavaRDD<File> preprocessedBAMFiles = preprocessSAMFiles(pathToSAMFiles, pathToReference, knownSites,
                toolsExtraArguments);

        if (preprocessedBAMFiles != null) {
            JavaRDD<File> variantsVCFFiles = discoverVariants(preprocessedBAMFiles, pathToReference,
                                                              toolsExtraArguments);
            List<File> outputFiles = variantsVCFFiles.collect();
            return outputFiles;
        } else {
            System.err.println("Could not preprocess SAM files!");
            return null;
        }
    }

    public static Options initCommandLineOptions() {
        Options options = new Options();

        Option reference = new Option("R", "Reference", true, "The path to the reference file.");
        reference.setRequired(true);
        options.addOption(reference);

        Option inputFolder = new Option("I", "InputFolder", true, "The path to the folder containing the input files.");
        inputFolder.setRequired(true);
        options.addOption(inputFolder);

        Option knownSites = new Option("S", "KnownSites", true, "The path to the file containing known sites (used in BQSR).");
        knownSites.setRequired(true);
        options.addOption(knownSites);

        Option configFile = new Option("C", "ConfigFile", true, "The path to the file configuration file.");
        configFile.setRequired(true);
        options.addOption(configFile);

        return options;
    }

    public static CommandLine parseCommandLineOptions(Options options, String[] argv) {
        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, argv);
        } catch (ParseException e) {
            System.out.println(e.getMessage());

            System.exit(1);
            return null;
        }

        return cmd;
    }

    public static void main(String argv[]) throws Exception {

        Options options = SparkCaller.initCommandLineOptions();
        CommandLine cmdArgs = SparkCaller.parseCommandLineOptions(options, argv);

        JavaSparkContext sparkContext = SparkCaller.initSpark("SparkCaller");
        SparkCaller caller = new SparkCaller(sparkContext);

        String pathToReference = cmdArgs.getOptionValue("Reference");
        String pathToSAMFiles = cmdArgs.getOptionValue("InputFolder");
        String knownSites = cmdArgs.getOptionValue("KnownSites");
        String configFilepath = cmdArgs.getOptionValue("ConfigFile");
        Properties toolsExtraArguments = Utils.loadConfigFile(configFilepath);

        caller.runPipeline(pathToSAMFiles, pathToReference, knownSites, toolsExtraArguments);
    }

    public static JavaSparkContext initSpark(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        return sparkContext;
    }
}
