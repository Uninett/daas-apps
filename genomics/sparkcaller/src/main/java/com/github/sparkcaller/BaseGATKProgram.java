package com.github.sparkcaller;

import org.broadinstitute.gatk.engine.CommandLineGATK;

import java.io.Serializable;
import java.util.ArrayList;

public class BaseGATKProgram implements Serializable {
    final protected ArrayList<String> extraArgs;
    private ArrayList<String> programArgs;

    public BaseGATKProgram(String toolkit, String extraArgsString) {
        this.extraArgs = Utils.possibleStringToArgs(extraArgsString);
        this.programArgs = new ArrayList<String>();

        addArgument("-T", toolkit);

        if (this.extraArgs != null) {
            programArgs.addAll(extraArgs);
        }
    }

    protected void executeProgram() throws Exception {
        CommandLineGATK.start(new CommandLineGATK(), this.programArgs.toArray(new String[0]));
    }

    protected void setThreads(String availableCoresPerNode) {
        // Most of the GATK tools works better by increasing the number of CPU threads allocated
        // to each data thread, so this is what we set by default.
        addArgument("-nct", availableCoresPerNode);
    }

    protected void setInputFile(String inputFilepath) {
        addArgument("-I", inputFilepath);
    }

    protected void setReference(String pathToReference) {
        addArgument("-R", pathToReference);
    }

    protected void setOutputFile(String outputPath) {
        addArgument("-o", outputPath);
    }

    protected void addArgument(String flag, String value) {
        programArgs.add(flag);
        if (value != null) {
            programArgs.add(value);
        }
    }
}
