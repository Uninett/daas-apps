import com.github.sparkbwa.BwaInterpreter;
import com.github.sparkbwa.BwaOptions;
import com.github.sparkcaller.SparkCaller;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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

        Option bwaAlgorithm = new Option("bwa", "BWAalg", true, "The algorithm to use when running BWA.");
        bwaAlgorithm.setRequired(true);
        options.addOption(bwaAlgorithm);

        Option reads = new Option("reads", "Reads", true, "Whether the reads are paired or single.");
        reads.setRequired(true);
        options.addOption(reads);

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

        Option availableNodes = new Option("N", "AvailableNodes", true, "The number of available nodes.");
        availableNodes.setRequired(true);
        options.addOption(availableNodes);

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
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        return sparkContext;
    }

    public static BwaOptions initSparkBwaOptions(CommandLine bwaOptions, Properties extraArgs) {
        String reference = bwaOptions.getOptionValue("Reference");
        String outputDir = bwaOptions.getOptionValue("OutputFolder");
        String bwaMode = bwaOptions.getOptionValue("Reads");
        String threads = bwaOptions.getOptionValue("CoresPerNode");
        String numNodes = bwaOptions.getOptionValue("AvailableNodes");
        String bwaAlg = bwaOptions.getOptionValue("BWAalg");
        String inputFastq[] = bwaOptions.getOptionValues("Input");

        BwaOptions sparkBwaOptions = new BwaOptions();
        if (bwaAlg.equals("aln")) {
            sparkBwaOptions.setAlnAlgorithm(true);
            sparkBwaOptions.setMemAlgorithm(false);
        } else if (bwaAlg.equals("bwasw")) {
            sparkBwaOptions.setBwaswAlgorithm(true);
            sparkBwaOptions.setMemAlgorithm(false);
        } else {
            sparkBwaOptions.setMemAlgorithm(true);
        }

        sparkBwaOptions.setIndexPath(reference);
        sparkBwaOptions.setPartitionNumber(Integer.parseInt(numNodes));

        String bwaArgs = extraArgs.getProperty("bwa");
        if (bwaArgs != null) {
            if (!bwaArgs.contains("-t")) {
                bwaArgs = bwaArgs + " -t " + threads;
            }
            sparkBwaOptions.setBwaArgs(bwaArgs);
        }

        if (bwaMode.equals("single")) {
            sparkBwaOptions.setSingleReads(true);
            sparkBwaOptions.setPairedReads(false);
        } else {
            sparkBwaOptions.setPairedReads(true);
        }

        sparkBwaOptions.setInputPath(inputFastq[0]);
        if (inputFastq.length == 2) {
            sparkBwaOptions.setInputPath2(inputFastq[0]);
        }

        sparkBwaOptions.setOutputPath(outputDir);

        return sparkBwaOptions;
    }

    public static BwaInterpreter initSparkBwa(JavaSparkContext sparkContext, CommandLine bwaOptions, Properties extraArgs) {
        BwaOptions sparkBwaOptions = initSparkBwaOptions(bwaOptions, extraArgs);
        BwaInterpreter bwaInterpreter = new BwaInterpreter(sparkBwaOptions, sparkContext.sc());

        return bwaInterpreter;
    }

    public static SparkCaller initSparkCaller(JavaSparkContext sparkContext, CommandLine gatkOptions, Properties extraArgs) {
        String pathToReference = gatkOptions.getOptionValue("Reference");
        String outputDirectory = gatkOptions.getOptionValue("OutputFolder");
        String knownSites = gatkOptions.getOptionValue("KnownSites");
        String configFilepath = gatkOptions.getOptionValue("ConfigFile");
        String coresPerNode = gatkOptions.getOptionValue("CoresPerNode");
        Properties toolsExtraArguments = com.github.sparkcaller.Utils.loadConfigFile(configFilepath);

        SparkCaller caller = new SparkCaller(sparkContext, pathToReference, knownSites, toolsExtraArguments,
                                             coresPerNode, outputDirectory);
        return caller;
    }

    public static void main(String argv[]) {
        Options options = Runner.initCommandLineOptions();
        CommandLine args = parseCommandLineOptions(options, argv);

        JavaSparkContext sparkContext = Runner.initSpark("Pipeline runner");

        String configFilepath = args.getOptionValue("ConfigFile");
        Properties toolsExtraArguments = com.github.sparkcaller.Utils.loadConfigFile(configFilepath);

        BwaInterpreter bwaInterpreter = Runner.initSparkBwa(sparkContext, args, toolsExtraArguments);
        bwaInterpreter.RunBwa();

        SparkCaller sparkCaller = initSparkCaller(sparkContext, args, toolsExtraArguments);

        String pathToSAMFiles = args.getOptionValue("OutputFolder");
        sparkCaller.runPipeline(pathToSAMFiles);
    }
}
