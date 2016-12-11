### Genomics
This folder contains implementations of genomics related tools.
All of the tools uses [Apache Spark](http://spark.apache.org/) to distribute
the workload.

Currently all of the tools does this by dividing the input into smaller chunks,
and then running the tools on each chunk. By doing so we are able to provide
a familiar interface, while (in most cases) improving the performance of each
tool.

### Tools
* **SparkAligner** is a short read aligner which can use
  [BWA](http://bio-bwa.sourceforge.net/) and Spark to
  perform alignment in a distributed manner. It is also possible to add
  support for new aligners.
* **SparkCaller** is used both preprocesses the file obtained from
  SparkAligner and does variant discovery. It uses the [GATK
  toolkit](https://www.google.no/search?q=GATK+bqsr&oq=GATK&aqs=chrome.0.69i59j69i57j69i60l4.431j0j1&sourceid=chrome&ie=UTF-8#safe=off&q=GATK+) to perform the
  analysis.
* **Pipeline runner** is a tool for running SparkAligner and SparkCaller as
  a single pipeline in a standalone JAR file.

### Requirements
* Indexed FASTA reference files (as normally required in the GATK pipeline).
* dbSNP files for the reference (the one for HG19 can be found by following the
  this [link](https://software.broadinstitute.org/gatk/download/bundle).
* It is preferred that NFS is used over HDFS, as GATK seemingly has some
  problem when writing to HDFS.

### Running the whole pipeline using pipeline-runner

This program can be used to run the entire pipeline shown below as a single
program. All the required dependencies (except Spark and scala) is packaged inside
the JAR file.

![GATK Workflow](img/spark_bio_workflow.png "Parts of the GATK workflow implemented
using Spark")

#### Usage
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

#### How to build the JAR for pipeline-runner
Run `cd pipeline-runner && mvn clean package` to build the JAR file.

### Benchmarks
The large dataset is the Illumina 100bp pair-ended exome 150x GCAT dataset.
small dataset refers to the Ion Torrent 225bp single-ended exome 30x GCAT
dataset. These datasets can be obtained
[here](https://f.128.no/gcat/).

Each data point is an average of three runs using the same settings.

SparkAligner should have the same performance as SparkBWA, as SparkAligner is
just a generalization of SparkBWA.


#### SparkBWA benchmarks
![SparkBWA small](img/bwa_small.png "The runtime of the BWA and SparkBWA
on the small dataset")

![SparkBWA large](img/bwa_large.png "The runtime of the BWA and SparkBWA
on the large dataset")

#### Whole pipeline benchmarks (SparkBWA + SparkCaller)
![Whole pipeline small](img/whole_pipeline_small.png "The runtime of the whole
pipeline on the small dataset")

![Whole pipeline large](img/whole_pipeline_large.png "The runtime of the whole
pipeline on the large dataset")
