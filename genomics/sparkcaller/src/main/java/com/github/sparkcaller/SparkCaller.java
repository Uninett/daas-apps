package com.github.sparkcaller;

import com.github.sparkcaller.preprocessing.*;
import com.github.sparkcaller.preprocessing.BAMIndexer;
import com.github.sparkcaller.utils.FileMover;
import com.github.sparkcaller.utils.SAMFileUtils;
import com.github.sparkcaller.utils.Utils;
import com.github.sparkcaller.utils.VCFFileUtils;
import com.github.sparkcaller.variantdiscovery.GenotypeGVCF;
import com.github.sparkcaller.variantdiscovery.HaplotypeCaller;
import com.github.sparkcaller.variantdiscovery.VQSRRecalibrationApplier;
import com.github.sparkcaller.variantdiscovery.VQSRTargetCreator;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;

import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SparkCaller {
    final private JavaSparkContext sparkContext;
    final private Logger log;
    private String pathToReference;
    private String knownSites;
    private Properties toolsExtraArgs;
    private String coresPerNode;
    private String outputFolder;

    /*
     * The SparkCaller is used for managing the workflow
     * @param pathToReference   the path to the file to use as a reference.
     *                         Keep in mind that this file has to be reachable by all nodes, and has to be indexed.
     *
     * @param pathToReference   the path to the file containing the dbsnp to use when ex. performing BQSR.
     *                         Keep in mind that this file has to be reachable by all nodes.
     *
     * @param toolsExtraArguments   the Properties object containing strings of extra arguments to pass to each tool.
     *
     */
    public SparkCaller(JavaSparkContext sparkContext, String pathToReference, String knownSites,
                       Properties toolsExtraArguments, String coresPerNode, String outputFolder) {

        this.sparkContext = sparkContext;
        this.log = Logger.getLogger(this.getClass());

        this.pathToReference = pathToReference;
        this.toolsExtraArgs = toolsExtraArguments;
        this.knownSites = knownSites;
        this.coresPerNode = coresPerNode;
        this.outputFolder = outputFolder;
    }

    public SparkCaller(SparkContext sparkContext, String pathToReference, String knownSites,
                       Properties toolsExtraArguments, String coresPerNode, String outputFolder) {

        this.sparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        this.log = Logger.getLogger(this.getClass());

        this.pathToReference = pathToReference;
        this.toolsExtraArgs = toolsExtraArguments;
        this.knownSites = knownSites;
        this.coresPerNode = coresPerNode;
        this.outputFolder = outputFolder;
    }

    public File maybeConvertToSortedBAM(ArrayList<File> samFiles) {
        String sortSamExtraArgs = this.toolsExtraArgs.getProperty("SortSam");
        List<File> bamFiles;

        if (sortSamExtraArgs != null) {
            this.log.info("Distributing the SAM files to the nodes...");
            JavaRDD<File> samFilesRDD = this.sparkContext.parallelize(samFiles);

            this.log.info("Converting the SAM files to sorted BAM files...");
            bamFiles = samFilesRDD.map(new SAMToSortedBAM()).map(new FileMover(this.outputFolder)).collect();
        }  else {
            bamFiles = samFiles;
        }

        try {
            return SAMFileUtils.mergeBAMFiles(bamFiles, this.outputFolder, "merged-sorted");
        } catch (IOException e) {
            this.log.error("Could not merge files!");
            e.printStackTrace();
            System.exit(1);
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    public File maybeMarkDuplicates(File bamFile) throws Exception {
        String markDuplicatesExtraArgs = this.toolsExtraArgs.getProperty("MarkDuplicates");

        if (markDuplicatesExtraArgs != null) {
            this.log.info("Marking duplicates...");
            return DuplicateMarker.markDuplicates(bamFile, this.outputFolder, markDuplicatesExtraArgs);
        }

        this.log.info("Skipping mark duplicates! Args for MarkDuplicates was not provided.");
        return bamFile;
    }

    public File maybeAddOrReplaceRG(File bamFile) throws Exception {
        String addOrReplaceExtraArgs = this.toolsExtraArgs.getProperty("AddOrReplaceReadGroups");

        if (addOrReplaceExtraArgs != null) {
            this.log.info("Adding read groups...");
            File bamWithRG = SAMFileUtils.addOrReplaceRG(bamFile, this.outputFolder, addOrReplaceExtraArgs);
            return bamWithRG;
        }

        this.log.info("Skipping AddOrReplaceReadGroups! Args for AddOrReplaceReadGroups was not provided.");
        return bamFile;
    }

    /* Performs the preprocessing stage of the GATK pipeline.
     * This is performed in a simple scatter-gather manner.
     * See the following link for details: https://www.broadinstitute.org/gatk/guide/bp_step.php?p=1
     *
     * @param pathToSAMFiles   the path to the folder containing the SAM files created by the aligner.
     *
    */
    public File preprocessSAMFiles(ArrayList<File> samFiles) throws Exception {

        this.log.info("Preprocessing SAM files!");
        if (samFiles != null) {
            File mergedBAMFile = maybeConvertToSortedBAM(samFiles);
            File BAMWithRG = maybeAddOrReplaceRG(mergedBAMFile);

            File dedupedBAMFile = maybeMarkDuplicates(BAMWithRG);
            File realignedBAMFile = maybeRealignIndels(dedupedBAMFile);
            File recalibratedBAMFile = maybePerformBQSR(realignedBAMFile);

            this.log.info("Preprocessing finished!");
            return recalibratedBAMFile;
        }

        return null;
    }

    public File maybePerformBQSR(File bamFile) throws Exception {

        String baseRecalibratorExtraArgs = this.toolsExtraArgs.getProperty("BaseRecalibrator");
        String printReadsExtraArgs = this.toolsExtraArgs.getProperty("PrintReads");

        if (baseRecalibratorExtraArgs != null && printReadsExtraArgs != null) {
            this.log.info("Creating targets on which to perform BQSR...");
            BQSRTargetGenerator bqsrTargetGenerator = new BQSRTargetGenerator(this.pathToReference,
                    this.knownSites,
                    this.outputFolder,
                    baseRecalibratorExtraArgs,
                    this.coresPerNode);

            File bqsrTargets = bqsrTargetGenerator.generateTargets(bamFile);

            JavaPairRDD<String, File> bamFilesRDD = splitByChromosomeAndCreateIndex(bamFile);

            this.log.info("Performing BQSR...");
            JavaRDD<File> recalibratedBAMFilesRDD = bamFilesRDD.map(new BQSR(this.pathToReference,
                    bqsrTargets.getPath(),
                    printReadsExtraArgs,
                    this.coresPerNode));
            return  SAMFileUtils.mergeBAMFiles(recalibratedBAMFilesRDD.collect(), this.outputFolder, "merged-bqsr");
        }

        this.log.info("Skipping BQSR! Args for BaseRecalibrator or/and PrintReads was not provided.");
        return bamFile;
    }

    private JavaPairRDD<String, File> splitByChromosomeAndCreateIndex(File inputBAMFile) throws IOException {
        List<Tuple2<String, File>> bamsByContigWithName = SAMFileUtils.splitBAMByChromosome(inputBAMFile, this.outputFolder);
        JavaPairRDD<String, File> bamsByContigRDD = this.sparkContext.parallelizePairs(bamsByContigWithName);
        bamsByContigRDD.mapValues(new BAMIndexer()).collect();

        return bamsByContigRDD.repartition(sparkContext.defaultParallelism());
    }

    public File maybeRealignIndels(File bamFile) throws Exception {
        String realignerTargetCreatorExtraArgs = this.toolsExtraArgs.getProperty("RealignerTargetCreator");
        String indelRealignerExtraArgs = this.toolsExtraArgs.getProperty("IndelRealigner");

        if (indelRealignerExtraArgs != null && realignerTargetCreatorExtraArgs != null) {
            this.log.info("Creating indel targets...");
            IndelTargetCreator indelTargetCreator = new IndelTargetCreator(this.pathToReference,
                                                                           this.outputFolder,
                                                                           realignerTargetCreatorExtraArgs,
                                                                           this.coresPerNode);
            File indelTargets = indelTargetCreator.createTargets(bamFile);

            this.log.info("Splitting BAMs by chromosome...");
            JavaPairRDD<String, File> bamsByContigRDD = splitByChromosomeAndCreateIndex(bamFile);

            this.log.info("Realigning indels...");
            JavaRDD<File> realignedIndels = bamsByContigRDD.map(new RealignIndels(this.pathToReference,
                    indelTargets,
                    indelRealignerExtraArgs));

            return SAMFileUtils.mergeBAMFiles(realignedIndels.collect(), this.outputFolder, "merged-realigned");
        }

        this.log.info("Skipping indel realignment! Args for RealingerTargetCreator and/or IndelRealinger was not provided.");
        return bamFile;
    }

    /* Performs the variant discovery stage of the GATK pipeline.
     * See the following link for details: https://www.broadinstitute.org/gatk/guide/bp_step.php?p=2
     *
     * @param preprocessedBAMFiles   a spark RDD containing the File object for each preprocessed BAM file.
     *
    */
    public File discoverVariants(File preprocessedBAMFile) throws IOException {
        this.log.info("Starting variant discovery!");
        this.log.info("Running HaplotypeCaller...");
        File variantsFile = maybePerformHaplotypeCalling(preprocessedBAMFile);

        ArrayList<File> variantsFiles = new ArrayList<>();
        variantsFiles.add(variantsFile);

        if (variantsFile != null) {
            return maybePerformJointGenotyping(variantsFiles);
        } else {
            return null;
        }
    }

    public File discoverVariants(List<File> variantsFiles) throws IOException {
        this.log.info("Starting variant discovery!");
        return maybePerformJointGenotyping(variantsFiles);
    }

    private File maybePerformHaplotypeCalling(File preprocessedBAMFile) throws IOException {
        String haplotypeCallerExtraArgs = this.toolsExtraArgs.getProperty("HaplotypeCaller");

        if (haplotypeCallerExtraArgs != null) {
            JavaPairRDD<String, File> bamsByContigRDD = splitByChromosomeAndCreateIndex(preprocessedBAMFile);
            JavaRDD<File> variantsVCFFilesRDD = bamsByContigRDD.map(new HaplotypeCaller(this.pathToReference,
                    haplotypeCallerExtraArgs,
                    this.coresPerNode));
            List<File> variantFiles = variantsVCFFilesRDD.map(new FileMover(this.outputFolder)).collect();
            File mergedVcfs = VCFFileUtils.mergeVCFFiles(variantFiles, "merged-hap");
            return Utils.moveToDir(mergedVcfs, this.outputFolder);
        }

        this.log.info("Skipping haplotype callijng! Args for HaplotypeCaller was not provided.");
        return null;
    }

    private File maybePerformJointGenotyping(List<File> variantFiles) {
        String extraArgs = this.toolsExtraArgs.getProperty("GenotypeGVCFs");

        if (extraArgs != null) {
            this.log.info("Performing joint genotyping...");
            GenotypeGVCF genotypeGVCF = new GenotypeGVCF(this.pathToReference,
                    extraArgs,
                    this.coresPerNode);
            try {
                File outputFile = new File(this.outputFolder, "merged.vcf");
                File mergedVariants = genotypeGVCF.performJointGenotyping(variantFiles, outputFile.getPath());

                this.log.info("Recalibrating variants...");
                File recalibratedVariants = recalibrateVariants(mergedVariants);

                return Utils.moveToDir(recalibratedVariants, this.outputFolder);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
                return null;
            }
        }

        return null;
    }

    private File maybePerformVariantTargetCreation(File vcfToRecalibrate, String extraArgs, String mode) throws Exception {
        if (extraArgs != null) {
            VQSRRecalibrationApplier vqsrApplier = new VQSRRecalibrationApplier(this.pathToReference,
                    this.toolsExtraArgs.getProperty("ApplyRecalibration"),
                    this.coresPerNode);

            VQSRTargetCreator targetCreator = new VQSRTargetCreator(this.pathToReference, extraArgs, this.coresPerNode);
            Tuple2<File, File> snpTargets = targetCreator.createTargets(vcfToRecalibrate, mode);

            return vqsrApplier.applyRecalibration(vcfToRecalibrate, snpTargets, mode);
        } else {
            return vcfToRecalibrate;
        }

    }

    private File recalibrateVariants(File vcfToRecalibrate) {
        String INDELextraArgs = this.toolsExtraArgs.getProperty("INDELVariantRecalibrator");
        String SNPextraArgs = this.toolsExtraArgs.getProperty("SNPVariantRecalibrator");

        try {
            vcfToRecalibrate = maybePerformVariantTargetCreation(vcfToRecalibrate, SNPextraArgs, "SNP");
            vcfToRecalibrate = maybePerformVariantTargetCreation(vcfToRecalibrate, INDELextraArgs, "INDEL");

            return vcfToRecalibrate;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);

            return null;
        }
    }

    /*
     * Handles the initialization of the pipeline, as well as running the actual pipeline in the correct order.
     *
     * @param pathToSAMFiles   the path to the folder containing the SAM files created by the aligner.
     *
     */
    public File runPipeline(String pathToInputFiles, String inputFileFormat) {

        File vcfVariants = null;
        try {
            inputFileFormat = inputFileFormat.toLowerCase();
            ArrayList<File> inputFiles = Utils.getFilesInFolder(pathToInputFiles, inputFileFormat);

            if (inputFileFormat.equals("sam") || inputFileFormat.equals("bam")) {
                File preprocessedBAMFile = preprocessSAMFiles(inputFiles);

                if (preprocessedBAMFile != null) {
                    vcfVariants = discoverVariants(preprocessedBAMFile);
                } else {
                    System.err.println("Could not preprocess SAM files!");
                    System.exit(1);
                }

            } else if (inputFileFormat.equals("vcf")) {
                vcfVariants = discoverVariants(inputFiles);
            } else {
                System.err.println("Invalid input format: " + inputFileFormat + "! Must be SAM, BAM or VCF!");
            };
        } catch (Exception e) {
            e.printStackTrace();
        }

        return vcfVariants;
    }

    public static Options initCommandLineOptions() {
        Options options = new Options();

        Option reference = new Option("R", "Reference", true, "The path to the reference file.");
        reference.setRequired(true);
        options.addOption(reference);

        Option inputFolder = new Option("I", "InputFolder", true, "The path to the folder containing the input files.");
        inputFolder.setRequired(true);
        options.addOption(inputFolder);

        Option inputFormat = new Option("F", "InputFormat", true, "The input fileformat (SAM or BAM).");
        inputFormat.setRequired(true);
        options.addOption(inputFormat);

        Option outputFolder = new Option("O", "OutputFolder", true, "The path to the folder which will store the final output files.");
        outputFolder.setRequired(true);
        options.addOption(outputFolder);

        Option knownSites = new Option("S", "KnownSites", true, "The path to the file containing known sites (used in BQSR).");
        knownSites.setRequired(true);
        options.addOption(knownSites);

        Option configFile = new Option("C", "ConfigFile", true, "The path to the file configuration file.");
        configFile.setRequired(true);
        options.addOption(configFile);

        Option threads = new Option("CPN", "CoresPerNode", true, "The number of available cores per node.");
        threads.setRequired(true);
        options.addOption(threads);

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
            formatter.printHelp("Sparkcaller", "", options,
                                "See https://github.com/UNINETT/daas-apps/tree/master/genomics for documentation.",
                                true);

            System.exit(1);
            return null;
        }

        return cmd;
    }

    public static void main(String argv[]) throws Exception {
        Options options = SparkCaller.initCommandLineOptions();
        CommandLine cmdArgs = SparkCaller.parseCommandLineOptions(options, argv);

        JavaSparkContext sparkContext = SparkCaller.initSpark("SparkCaller");

        String pathToReference = cmdArgs.getOptionValue("Reference");
        String pathToSAMFiles = cmdArgs.getOptionValue("InputFolder");
        String inputFormat = cmdArgs.getOptionValue("InputFormat");
        String outputDirectory = cmdArgs.getOptionValue("OutputFolder");
        String knownSites = cmdArgs.getOptionValue("KnownSites");
        String configFilepath = cmdArgs.getOptionValue("ConfigFile");
        String coresPerNode = cmdArgs.getOptionValue("CoresPerNode");
        Properties toolsExtraArguments = Utils.loadConfigFile(configFilepath);

        SparkCaller caller = new SparkCaller(sparkContext, pathToReference, knownSites,
                                             toolsExtraArguments, coresPerNode, outputDirectory);
        caller.runPipeline(pathToSAMFiles, inputFormat);
    }

    public static JavaSparkContext initSpark(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        return sparkContext;
    }
}
