import com.github.sparkaligner.BaseAligner;
import com.github.sparkaligner.aligners.bwa.Bwa;
import com.github.sparkcaller.SparkCaller;
import com.github.sparkcaller.utils.MiscUtils;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;
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

        Option alignerName = new Option("A", "AlignerName", true, "The aligner to use when aligning FASTQ files.");
        alignerName.setRequired(false);
        options.addOption(alignerName);

        Option knownSites = new Option("S", "KnownSites", true, "The path to the known sites file (only required when running BQSR).");
        knownSites.setRequired(false);
        options.addOption(knownSites);

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
            formatter.printHelp("pipeline-runner", "", options, "", true);
            System.exit(1);
        }

        return cmd;
    }


    public static JavaSparkContext initSpark(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        return sparkContext;
    }

    public static SparkCaller initSparkCaller(JavaSparkContext sparkContext, CommandLine gatkOptions, Properties extraArgs) {
        String pathToReference = gatkOptions.getOptionValue("Reference");
        String knownSites = gatkOptions.getOptionValue("KnownSites");
        String coresPerNode = sparkContext.getConf().get("spark.executor.cores", "4");

        return new SparkCaller(sparkContext, pathToReference, knownSites, extraArgs, coresPerNode);
    }

    public static void main(String argv[]) throws IOException {
        JavaSparkContext sparkContext = Runner.initSpark("pipeline-runner");

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
        String alignerName = args.getOptionValue("AlignerName");
        // Default to BWA if no aligner was given.
        if (alignerName == null) {
            alignerName = "bwa";
        }

        BaseAligner aligner = null;
        switch (alignerName) {
            case "bwa":
                aligner = new Bwa(sparkContext, argv);
                break;
        }

        if (aligner != null) {
            aligner.run();
        } else {
            System.err.println(alignerName + " was not found!");
            System.exit(-2);
        }

        String pathToSAMFiles = args.getOptionValue("Input");
        SparkCaller sparkCaller = initSparkCaller(sparkContext, args, toolsExtraArguments);
        sparkCaller.runPipeline(pathToSAMFiles);
    }
}
