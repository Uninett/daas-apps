package com.github.sparkcaller.utils;

import org.broadinstitute.gatk.engine.CommandLineGATK;

import java.io.Serializable;
import java.util.ArrayList;

public class BaseGATKProgram implements Serializable {
    final protected ArrayList<String> extraArgs;
    private ArrayList<String> programArgs;

    public BaseGATKProgram(String toolkit, String extraArgsString) {
        this.extraArgs = MiscUtils.possibleStringToArgs(extraArgsString);
        this.programArgs = new ArrayList<String>();

        addArgument("-T", toolkit);

        if (this.extraArgs != null) {
            programArgs.addAll(extraArgs);
        }
    }

    protected void executeProgram() throws Exception {
        int numTries = 0;
        int maxRetries = 5;

        while (true) {
            try {
                CommandLineGATK.start(new CommandLineGATK(), this.programArgs.toArray(new String[0]));
                return;
            } catch (org.broadinstitute.gatk.utils.exceptions.ReviewedGATKException e) {
                if (++numTries == maxRetries) {
                    System.err.println("Failed to run GATK program " + maxRetries + " times!");
                    return;
                }
            }
        }
    }

    protected void setThreads(String availableCoresPerNode) {
        // Most of the GATK tools works better by increasing the number of CPU threads allocated
        // to each data thread, so this is what we set by default.
        addArgument("-nct", availableCoresPerNode);
    }

    protected void setInputFile(String inputFilepath) {
        changeArgument("-I", inputFilepath);
    }

    protected void setReference(String pathToReference) {
        changeArgument("-R", pathToReference);
    }

    protected void setOutputFile(String outputPath) {
        changeArgument("-o", outputPath);
    }

    protected void setInterval(String interval) {
        // 'unmapped' is not a valid interval, so do not attempt to use it.
        if (!interval.equals("unmapped")) {
            changeArgument("-L", interval);
        }
    }

    protected void addArgument(String flag, String value) {
        programArgs.add(flag);
        if (value != null) {
            programArgs.add(value);
        }
    }

    protected void changeArgument(String flag, String value) {
        int flagIndex = programArgs.indexOf(flag);

        // If the flag is not found, just add the argument.
        if (flagIndex < 0) {
            programArgs.add(flag);
            programArgs.add(value);
        } else {
            programArgs.set(flagIndex+1, value);
        }

    }
}
