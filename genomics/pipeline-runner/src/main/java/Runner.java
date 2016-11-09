import com.github.sparkbwa.BwaInterpreter;
import com.github.sparkbwa.BwaOptions;
import com.github.sparkcaller.SparkCaller;
import com.github.sparkcaller.utils.MiscUtils;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class Runner {

    public static Options initCommandLineOptions() {
        Options options = new Options();

        Option inputFiles = new Option("I", "Input", true, "The path to the input FASTQ file(s).");
        inputFiles.setArgs(2);
        inputFiles.setRequired(true);
        options.addOption(inputFiles);

        Option reference = new Option("R", "Reference", true, "The path to the reference file.");
        reference.setRequired(true);
        options.addOption(reference);

        Option knownSites = new Option("S", "KnownSits", true, "The path to the known sites file (only required when running BQSR).");
        knownSites.setRequired(false);
        options.addOption(knownSites);

        Option bwaAlgorithm = new Option("bwa", "BWAalg", true, "The algorithm to use when running BWA.");
        bwaAlgorithm.setRequired(false);
        options.addOption(bwaAlgorithm);

        Option configFile = new Option("C", "ConfigFile", true, "The path to the file configuration file.");
        configFile.setRequired(false);
        options.addOption(configFile);

        return options;
    }

    public static CommandLine parseCommandLineOptions(Options options, String[] argv) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, argv);
        } catch (ParseException e) {
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Pipeline runner", "", options, "", true);
            System.exit(1);
        }

        return cmd;
    }


    public static JavaSparkContext initSpark(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        return sparkContext;
    }

    public static ArrayList<String> getFilesInFolder(String pathToFolder) {
        File folder = new File(pathToFolder);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles != null) {
            ArrayList<String> fastqFiles = new ArrayList<String>();

            for (File file : listOfFiles) {
                String fileName = file.getName().toLowerCase();
                if (fileName.endsWith("fastq") || fileName.endsWith("bam")) {
                    fastqFiles.add(file.getPath());
                }
            }
            return fastqFiles;
        }

        throw new ValueException("The folder: %s is either empty or was not found!", folder.getName());
     }


    public static BwaOptions initSparkBwaOptions(CommandLine bwaOptions, Properties extraArgs, JavaSparkContext sparkContext) {
        String reference = bwaOptions.getOptionValue("Reference");
        String bwaAlg = bwaOptions.getOptionValue("BWAalg");
        String inputFastqFolder = bwaOptions.getOptionValue("Input");

        BwaOptions sparkBwaOptions = new BwaOptions();
        if (bwaAlg == null) {
            System.out.println("BWA algorithm not given! Defaulting to mem.");
        } else if (bwaAlg.equals("aln")) {
            sparkBwaOptions.setAlnAlgorithm(true);
            sparkBwaOptions.setMemAlgorithm(false);
        } else if (bwaAlg.equals("bwasw")) {
            sparkBwaOptions.setBwaswAlgorithm(true);
            sparkBwaOptions.setMemAlgorithm(false);
        } else {
            sparkBwaOptions.setMemAlgorithm(true);
        }

        List<String> files = getFilesInFolder(inputFastqFolder);
        String inputFilePath = files.get(0);
        sparkBwaOptions.setInputPath(inputFilePath);;
        System.out.println("Setting: " + inputFilePath + " as an input file.");

        if (files.size() == 2) {
            String inputFile2Path = files.get(1);
            sparkBwaOptions.setInputPath2(inputFile2Path);
            System.out.println("Setting: " + inputFile2Path + " as an input file.");
            sparkBwaOptions.setPairedReads(true);
            sparkBwaOptions.setSingleReads(false);
            System.out.println("Running in paired mode.");
        } else if (files.size() > 2) {
            throw new ValueException("More than two fastq files was found in: " + inputFastqFolder);
        } else {
            sparkBwaOptions.setSingleReads(true);
            sparkBwaOptions.setPairedReads(false);
            System.out.println("Running in single mode.");
        }

        sparkBwaOptions.setIndexPath(reference);

        sparkBwaOptions.setPartitionNumber(sparkContext.sc().getExecutorStorageStatus().length);
        String threads = sparkContext.getConf().get("spark.executor.cores", "4");

        String bwaArgs = extraArgs.getProperty("bwa");
        if (bwaArgs != null) {
            if (!bwaArgs.contains("-t")) {
                bwaArgs = bwaArgs + " -t " + threads;
            }
            sparkBwaOptions.setBwaArgs(bwaArgs);
        }

        String pipelineRunnerOutputPostfix = "runner-" + sparkContext.sc().applicationId();
        sparkBwaOptions.setOutputPath(inputFastqFolder + "/" + pipelineRunnerOutputPostfix);

        return sparkBwaOptions;
    }

    public static SparkCaller initSparkCaller(JavaSparkContext sparkContext, CommandLine gatkOptions, Properties extraArgs) {
        String pathToReference = gatkOptions.getOptionValue("Reference");
        String knownSites = gatkOptions.getOptionValue("KnownSites");
        String coresPerNode = sparkContext.getConf().get("spark.executor.cores", "4");

        return new SparkCaller(sparkContext, pathToReference, knownSites, extraArgs, coresPerNode);
    }

    public static void main(String argv[]) throws IOException {
        JavaSparkContext sparkContext = Runner.initSpark("Pipeline runner");

        Options options = Runner.initCommandLineOptions();
        CommandLine args = parseCommandLineOptions(options, argv);

        String configFilepath = args.getOptionValue("ConfigFile");
        Properties toolsExtraArguments;

        if (configFilepath != null) {
            toolsExtraArguments = MiscUtils.loadConfigFile(configFilepath);
        } else {
            toolsExtraArguments = new Properties();
            try {
                System.out.println("Using default arguments.");
                toolsExtraArguments.load(Runner.class.getResourceAsStream("/default_args.properties"));
            } catch (IOException e) {
                e.printStackTrace();
                throw new IOException("Could not read properties file!");
            }
        }


        BwaOptions sparkBwaOptions = initSparkBwaOptions(args, toolsExtraArguments, sparkContext);
        BwaInterpreter bwaInterpreter = new BwaInterpreter(sparkBwaOptions, sparkContext.sc());
        bwaInterpreter.runBwa();

        SparkCaller sparkCaller = initSparkCaller(sparkContext, args, toolsExtraArguments);

        String pathToSAMFiles = sparkBwaOptions.getOutputPath();
        sparkCaller.runPipeline(pathToSAMFiles);
    }
}
