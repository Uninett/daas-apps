package com.github.sparkcaller;

import com.github.sparkcaller.preprocessing.*;
import com.github.sparkcaller.preprocessing.BAMIndexer;
import com.github.sparkcaller.utils.*;
import com.github.sparkcaller.variantdiscovery.HaplotypeCaller;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
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
import java.util.*;

public class SparkCaller {
    final private JavaSparkContext sparkContext;
    final private Logger log;
    private String pathToReference;
    private String knownSites;
    private Properties toolsExtraArgs;
    private String coresPerNode;
    private String outputFolderPostfix;

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
                       Properties toolsExtraArguments, String coresPerNode) {

        this.sparkContext = sparkContext;
        this.log = Logger.getLogger(this.getClass());

        this.pathToReference = pathToReference;
        this.toolsExtraArgs = toolsExtraArguments;
        this.knownSites = knownSites;
        this.coresPerNode = coresPerNode;
        this.outputFolderPostfix = "sparkcaller-" + sparkContext.sc().applicationId();
    }

    public SparkCaller(SparkContext sparkContext, String pathToReference, String knownSites,
                       Properties toolsExtraArguments, String coresPerNode) {

        this.sparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        this.log = Logger.getLogger(this.getClass());

        this.pathToReference = pathToReference;
        this.toolsExtraArgs = toolsExtraArguments;
        this.knownSites = knownSites;
        this.coresPerNode = coresPerNode;
        this.outputFolderPostfix = "sparkcaller-" + sparkContext.applicationId();
    }

    public List<File> maybeConvertToSortedBAM(ArrayList<Tuple2<File, File>> samFiles) {
        String sortSamExtraArgs = this.toolsExtraArgs.getProperty("SortSam");

        this.log.info("Distributing the SAM files to the nodes...");

        if (sortSamExtraArgs != null) {
            this.log.info("Converting the SAM files to sorted BAM files...");

            JavaRDD<Tuple2<File, File>> bamFilesRDD = this.sparkContext.parallelize(samFiles, samFiles.size());
            bamFilesRDD = bamFilesRDD.map(new SAMToSortedBAM());
            return mergeBAMsAndMove(bamFilesRDD, "merged-sorted");
        } else {
            List<File> outputBamFiles = new ArrayList<File>();
            for (Tuple2<File, File> inputOutputTuple : samFiles) {
                outputBamFiles.add(inputOutputTuple._2);
            }

            return outputBamFiles;
        }
    }

    public List<File> maybeMarkDuplicates(List<File> bamFiles) throws Exception {
        String markDuplicatesExtraArgs = this.toolsExtraArgs.getProperty("MarkDuplicates");

        if (markDuplicatesExtraArgs != null) {
            this.log.info("Marking duplicates...");
            JavaRDD<File> bamFilesRDD = this.sparkContext.parallelize(bamFiles, bamFiles.size());
            return bamFilesRDD.map(new DuplicateMarker(markDuplicatesExtraArgs)).map(new FileMover(this.outputFolderPostfix)).collect();
        }

        this.log.info("Skipping mark duplicates! Args for MarkDuplicates was not provided.");
        return bamFiles;
    }

    public List<File> maybeAddOrReplaceRG(List<File> bamFiles) throws Exception {
        String addOrReplaceExtraArgs = this.toolsExtraArgs.getProperty("AddOrReplaceReadGroups");

        if (addOrReplaceExtraArgs != null) {
            this.log.info("Adding read groups...");
            JavaRDD<File> bamFilesRDD = this.sparkContext.parallelize(bamFiles, bamFiles.size());
            return bamFilesRDD.map(new AddOrReplaceRGs(addOrReplaceExtraArgs)).map(new FileMover(this.outputFolderPostfix)).collect();
        }

        this.log.info("Skipping AddOrReplaceReadGroups! Args for AddOrReplaceReadGroups was not provided.");
        return bamFiles;
    }

    /* Performs the preprocessing stage of the GATK pipeline.
     * This is performed in a simple scatter-gather manner.
     * See the following link for details: https://www.broadinstitute.org/gatk/guide/bp_step.php?p=1
     *
     * @param pathToSAMFiles   the path to the folder containing the SAM files created by the aligner.
     *
    */
    public List<File> preprocessSAMFiles(ArrayList<Tuple2<File, File>> samFiles) throws Exception {

        this.log.info("Preprocessing SAM files!");
        if (samFiles != null) {
            List<File> mergedBAMFile = maybeConvertToSortedBAM(samFiles);
            List<File> BAMWithRG = maybeAddOrReplaceRG(mergedBAMFile);

            List<File> dedupedBAMFile = maybeMarkDuplicates(BAMWithRG);
            List<File> realignedBAMFile = maybeRealignIndels(dedupedBAMFile);
            List<File> recalibratedBAMFile = maybePerformBQSR(realignedBAMFile);

            this.log.info("Preprocessing finished!");
            return recalibratedBAMFile;
        }

        return null;
    }

    public List<File> maybePerformBQSR(List<File> bamFiles) throws Exception {

        String baseRecalibratorExtraArgs = this.toolsExtraArgs.getProperty("BaseRecalibrator");
        String printReadsExtraArgs = this.toolsExtraArgs.getProperty("PrintReads");

        if (baseRecalibratorExtraArgs != null && printReadsExtraArgs != null) {
            this.log.info("Creating targets on which to perform BQSR...");

            JavaRDD<File> bamFilesRDD = this.sparkContext.parallelize(bamFiles, bamFiles.size());
            List<File> bqsrTargets = bamFilesRDD.map(new BQSRTargetGenerator(this.pathToReference, this.knownSites,
                                                                             baseRecalibratorExtraArgs, this.coresPerNode))
                                                .map(new FileMover(this.outputFolderPostfix)).collect();

            HashMap<String, File> bqsrTargetsMapper = createTargetMapping(bqsrTargets);

            JavaPairRDD<String, Tuple2<File, File>> bamsByChromosome = splitByChromosomeAndCreateIndex(bamFiles);

            this.log.info("Performing BQSR...");
            return mergeBAMsAndMove(bamsByChromosome.map(new BQSR(this.pathToReference, bqsrTargetsMapper,
                                                              printReadsExtraArgs,
                                                              this.coresPerNode)), "merged-bqsr");
        }

        this.log.info("Skipping BQSR! Args for BaseRecalibrator or/and PrintReads was not provided.");
        return bamFiles;
    }

    private List<File> mergeBAMsAndMove(JavaRDD<Tuple2<File, File>> inputRDD, String mergedFileName) {
        return inputRDD.map(new FileMover(this.outputFolderPostfix)).groupBy(File::getParent).map(new BAMMerger(mergedFileName, this.coresPerNode))
                                                      .map(new FileMover(this.outputFolderPostfix)).map(BAMIndexer::indexBAM).collect();
    }

    private JavaPairRDD<String, Tuple2<File, File>> splitByChromosomeAndCreateIndex(List<File> inputBAMFiles) throws IOException {
        this.log.info("Splitting BAM by chromosome...");
        ArrayList<Tuple2<File, List<SAMSequenceRecord>>> samFileSequences = new ArrayList<>();
        int totalNumContigs = 0;

        for (File inputFile : inputBAMFiles) {
            SamReader samFile = SamReaderFactory.makeDefault().open(inputFile);
            SAMFileHeader samFileHeader = samFile.getFileHeader();
            List<SAMSequenceRecord> allContigs = samFileHeader.getSequenceDictionary().getSequences();
            totalNumContigs += allContigs.size();

            samFileSequences.add(new Tuple2<>(inputFile, allContigs));
        }


        // Use the length of the contigs to determine which partition they should be in.
        final int coresPerTask = this.sparkContext.getConf().getInt("spark.task.cpus", 4);
        final int numCoresPerNode = Integer.parseInt(this.coresPerNode);

        if (numCoresPerNode <= 0) {
            this.log.info("Invalid number of cores per node: " + numCoresPerNode + " must be a positive integer.");
            System.exit(1);
        }

        int contigsPerPartition = numCoresPerNode / coresPerTask;
        Map<String, Integer> contigPartitionMapping = new HashMap<>();

        if (contigsPerPartition <= 0) {
            this.log.info("Invalid number of contigs per partition! Defaulting to 4.");
            contigsPerPartition = 4;
        }

        final int numPartitions = (int) Math.ceil(totalNumContigs / contigsPerPartition);

        long[] contigLengthPartitions = new long[numPartitions];
        for (int i = 0; i < contigLengthPartitions.length; i++) {
            contigLengthPartitions[i] = 0;
        }

        List<Tuple2<String, Tuple2<File, File>>> contigsName = new ArrayList<>();
        for (Tuple2<File, List<SAMSequenceRecord>> fileContigsTuple : samFileSequences) {
            File inputBAMFile = fileContigsTuple._1;
            List<SAMSequenceRecord> allContigs = fileContigsTuple._2;

            String baseContigFilename = MiscUtils.removeExtenstion(inputBAMFile.getPath(), "bam");
            for (SAMSequenceRecord contig : allContigs) {
                long currMin = -1;
                int currMinIndex = -1;

                // Find the partition with the smallest total size
                for (int i = 0; i < contigLengthPartitions.length; i++) {
                    long partitionSize = contigLengthPartitions[i];

                    if (currMin == -1 || partitionSize < currMin) {
                        currMin = partitionSize;
                        currMinIndex = i;
                    }
                }

                // Place the contig in the partition which currently is the smallest
                contigLengthPartitions[currMinIndex] +=  contig.getSequenceLength();
                String sequenceName = contig.getSequenceName();
                contigPartitionMapping.put(sequenceName, currMinIndex);

                String outputFilename = baseContigFilename + "-" + sequenceName + ".bam";
                Tuple2<File, File> inputFileContig = new Tuple2<>(inputBAMFile,  new File(outputFilename));
                contigsName.add(new Tuple2<>(sequenceName, inputFileContig));
            }
        }

        JavaPairRDD<String, Tuple2<File, File>> bamsByContigWithName = this.sparkContext.parallelizePairs(contigsName, contigsName.size());
        return bamsByContigWithName.partitionBy(new BinPartitioner(numPartitions, contigPartitionMapping))
                                   .mapToPair(new SAMSplitter(this.coresPerNode))
                                   .mapValues(new BAMIndexer());
    }

    public List<File> maybeRealignIndels(List<File> bamFiles) throws Exception {
        String realignerTargetCreatorExtraArgs = this.toolsExtraArgs.getProperty("RealignerTargetCreator");
        String indelRealignerExtraArgs = this.toolsExtraArgs.getProperty("IndelRealigner");

        if (indelRealignerExtraArgs != null && realignerTargetCreatorExtraArgs != null) {
            this.log.info("Creating indel targets...");
            JavaRDD<File> bamFilesRDD = this.sparkContext.parallelize(bamFiles, bamFiles.size()).map(BAMIndexer::indexBAM);
            List<File> indelTargets = bamFilesRDD.map(new IndelTargetCreator(this.pathToReference,
                                                      realignerTargetCreatorExtraArgs, this.coresPerNode))
                                                 .map(new FileMover(this.outputFolderPostfix)).collect();

            HashMap<String, File> indelTargetsMapper = createTargetMapping(indelTargets);

            this.log.info("Splitting BAMs by chromosome...");
            JavaPairRDD<String, Tuple2<File, File>> bamsByContigRDD = splitByChromosomeAndCreateIndex(bamFiles);

            this.log.info("Realigning indels...");
            return mergeBAMsAndMove(bamsByContigRDD.map(new RealignIndels(this.pathToReference, indelTargetsMapper,
                                                         indelRealignerExtraArgs)), "merged-realigned");
        }

        this.log.info("Skipping indel realignment! Args for RealingerTargetCreator and/or IndelRealinger was not provided.");
        return bamFiles;
    }

    private HashMap<String, File> createTargetMapping(List<File> targets) {
        HashMap<String, File> targetsMapper = new HashMap<>();
        for (File target : targets) {
            targetsMapper.put(target.getParentFile().getParent(), target);
        }

        return targetsMapper;
    }

    /* Performs the variant discovery stage of the GATK pipeline.
     * See the following link for details: https://www.broadinstitute.org/gatk/guide/bp_step.php?p=2
     *
     * @param preprocessedBAMFiles   a spark RDD containing the File object for each preprocessed BAM file.
     *
    */
    public List<File> discoverVariants(List<File> preprocessedBAMFiles) throws IOException {
        this.log.info("Starting variant discovery!");
        return maybePerformHaplotypeCalling(preprocessedBAMFiles);
    }

    private List<File> maybePerformHaplotypeCalling(List<File> preprocessedBAMFiles) throws IOException {
        String haplotypeCallerExtraArgs = this.toolsExtraArgs.getProperty("HaplotypeCaller");

        if (haplotypeCallerExtraArgs != null) {
            this.log.info("Running HaplotypeCaller...");
            JavaPairRDD<String, Tuple2<File, File>> bamsByContigRDD = splitByChromosomeAndCreateIndex(preprocessedBAMFiles);

            JavaRDD<Tuple2<File, File>> variantsVCFFilesRDD = bamsByContigRDD.map(new HaplotypeCaller(this.pathToReference,
                    haplotypeCallerExtraArgs,
                    this.coresPerNode));

            List<File> mergedVcfs = variantsVCFFilesRDD.map(new FileMover(this.outputFolderPostfix))
                                                       .groupBy(File::getParent)
                                                       .map(new VCFMerger("merged-hap"))
                                                       .map(new FileMover(this.outputFolderPostfix)).collect();

            return mergedVcfs;
        }

        this.log.info("Skipping haplotype calling! Args for HaplotypeCaller was not provided.");
        return null;
    }

    /*
     * Handles the initialization of the pipeline, as well as running the actual pipeline in the correct order.
     *
     * @param pathToSAMFiles   the path to the folder containing the SAM files created by the aligner.
     *
     */
    public List<File> runPipeline(String pathToInputFiles) {

        List<File> vcfVariants = null;
        try {
            ArrayList<Tuple2<File, File>> inputFiles = MiscUtils.getFilesInFolder(pathToInputFiles);

            List<File> preprocessedBAMFile = preprocessSAMFiles(inputFiles);

            if (preprocessedBAMFile != null) {
                vcfVariants = discoverVariants(preprocessedBAMFile);
            } else {
                System.err.println("Could not preprocess SAM files!");
                System.exit(1);
            }
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

        Option knownSites = new Option("S", "KnownSites", true, "The path to the file containing known sites (used in BQSR).");
        knownSites.setRequired(false);
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
        String knownSites = cmdArgs.getOptionValue("KnownSites");
        String configFilepath = cmdArgs.getOptionValue("ConfigFile");
        Properties toolsExtraArguments = MiscUtils.loadConfigFile(configFilepath);

        if (knownSites == null && toolsExtraArguments.get("BaseRecalibrator") != null) {
            System.out.println("-S <Path to dbsnp> must be set if BQSR is to be performed, as this is required by the GATK toolkit.");
            System.exit(1);
        }

        String coresPerNode = sparkContext.getConf().get("spark.executor.cores", "4");

        if (coresPerNode == null) {
            System.err.println("The spark.executor.cores setting is not set!");
            System.err.println("spark.executor.cores is required to determine how many cores to use when distributing tasks.");
            System.err.println("Exiting!");
            System.exit(1);
        }

        SparkCaller caller = new SparkCaller(sparkContext, pathToReference, knownSites, toolsExtraArguments,
                                             coresPerNode);
        caller.runPipeline(pathToSAMFiles);
        caller.log.info("Closing spark context!");

        sparkContext.stop();
    }

    public static JavaSparkContext initSpark(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        return new JavaSparkContext(conf);
    }



}
