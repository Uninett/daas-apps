# Pipeline runner

This program can be used to run the entire pipeline shown below as a single
program. All the required dependencies (except Spark and scala) is packaged inside
the JAR file.

![GATK Workflow](../img/spark_bio_workflow.png "Parts of the GATK workflow
implemented using Spark")

### Usage
The only arguments that are required is the path to the input folder and the
path to the reference file. If you want to run BQSR, you also have to provide
a dbSNP file.

Example usage:
```
spark-submit                                        \
--class Runner                                      \
pipeline-runner-1.0.jar                             \
-R <Path to reference file>                         \
-I <Path to the folder containing the FASTQ files>  \
[-C <Path to config file>] # Defaults to 'default_args.properties' if not given
[-S <Path to known sites>] # Only required when BQSR should be performed
```

Arguments for all of the tools in the pipeline can be provided by specifying
a config file. See `config_example.properties` to see how this file is
structured.

### How to build
Run `mvn clean package` to build the JAR file.
