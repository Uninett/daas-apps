#### Table of Contents
* *[How it works](#how-it-works)*
* *[Configuration per tool](#configuration-per-tool)*
* *[Arguments already specified by SparkCaller](#arguments-already-specified-by-sparkcaller)*
* *[Usage](#usage)*
* *[Recommended Spark settings](#recommended-spark-settings)*
* *[How to build](#how-to-build-this-project)*
* *[Dependencies](#dependencies)*

### How it works
The SparkCaller exploits the fact that several commonly used tools in the GATK
pipeline can be used on each chromosome. The tools that are scatter-gatherable
in this manner is mentioned in [the GATK parallelism
guide](http://gatkforums.broadinstitute.org/dsde/discussion/1975/how-can-i-use-parallelism-to-make-gatk-tools-run-faster).
PrintReads can also be scatter-gathered if BaseRecalibrator is *not* used per
chromosome. [Apache Spark](http://spark.apache.org/) is used to distribute the
tasks to the nodes.

![SparkCaller pipeline](img/sparkcaller_pipeline.png "How the pipeline is run
using the SparkCaller")

### Configuration per tool
It is possible to pass arbitrary arguments to each tool in the GATK toolkit.

Keep in mind that the tools which are not present in the configuration file
will be skipped.

The file `config_example.properties` is an example config file where the entire
pipeline is executed.

The name which GATK uses for the tool is used as the key. The following keys
are valid:

* SortSam (currently does not accept extra arguments)
* [AddOrReplaceReadGroups](https://broadinstitute.github.io/picard/command-line-overview.html#AddOrReplaceReadGroups)
* [MarkDuplicates](https://broadinstitute.github.io/picard/command-line-overview.html#MarkDuplicates)
* [RealignerTargetCreator](https://software.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_indels_RealignerTargetCreator.php)
* [IndelRealigner](https://software.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_indels_IndelRealigner.php)
* [BaseRecalibrator](https://software.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_bqsr_BaseRecalibrator.php)
* [PrintReads](https://software.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_readutils_PrintReads.php)
* [HaplotypeCaller](https://software.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_haplotypecaller_HaplotypeCaller.php)

#### Arguments already specified by SparkCaller
The input, reference, and output arguments are set automatically for all tools.
* RealignerTargetCreator:
	* -nt
* IndelRealigner:
	* -targetIntervals
	* -L
* BaseRecalibrator
	* -knownSites
	* -nct
* PrintReads
	* -BQSR
	* -nct
	* -L
* HaplotypeCaller
	* -nct
	* -L

### Usage
SparkCaller is capable of processing several datasets at the same time. The
`-I` option is used to specify the folder where the input dataset(s) can be
found. If the input folder contains subfolders, each of these will be assumed
to be a seperate dataset. The output files are automatically stored in
a subfolder inside the folder where the dataset was read from.


```
spark-submit                                      \
--class com.github.sparkcaller.SparkCaller        \
--conf spark.executor.cores=<num cores per node>  \
--executor-memory <RAM per executor>              \
--driver-memory  <driver RAM>                     \
sparkcaller-1.0.jar                               \
-C <Path to config file>                          \
-R <Path to reference file>                       \
-I <Path to the folder containing the SAM files>  \
-S <Path to known sites>
```

Ex.
```
spark-submit                                      \
--class com.github.sparkcaller.SparkCaller        \
--conf spark.executor.cores=8                     \
--executor-memory 16G                             \
--driver-memory  6G                               \
sparkcaller-1.0.jar                               \
-C sparkcaller.properties                         \
-R /data/hdfs/1000genomes/hg19/ucsc.hg19.fasta    \
-I /data/hdfs/sparkcaller/sams/                   \
-S /data/hdfs/1000genomes/dbsnp/human/dbsnp_138.hg19.vcf
```

You can also use the sh script `submit-sparkcaller.sh` to start the job.
This script passes all arguments directly to the sparkcaller. Edit the file to
change the default Spark options.
The script can be used in the following way:

```
./submit-sparkcaller.sh                           \
-C sparkcaller.properties                         \
-R /data/hdfs/1000genomes/hg19/ucsc.hg19.fasta    \
-I /data/hdfs/sparkcaller/sams/                   \
-S /data/hdfs/1000genomes/dbsnp/human/dbsnp_138.hg19.vcf
```

### Useful data
Some useful data, such as the dbSNP and reference files, can be find at the [GSA
public FTP
server](http://gatkforums.broadinstitute.org/gatk/discussion/1215/how-can-i-access-the-gsa-public-ftp-server).

### Recommended Spark settings
TL;DR:
```
--conf spark.driver.maxResultSize = 0
--conf spark.executor.cores = (Number of available cores per node)
--conf spark.task.cpus = (~1/2 of spark.executor.cores)
```
SparkCaller uses the driver when merging BAM and VCF files, it is thus also
recommended to allocate up to 32 cores to the driver node.

It is also recommended to set `spark.driver.maxResultSize` to something high,
as the result may be large.

`spark.executor.cores` can be used to set how many cores to use per tool when
distributed to workers.  `spark.task.cpus` can be used to set how many cores to
use per task. It is recommended, based on experiments, to set this to about 1/2
of `spark.executor.cores`, but this varies with the dataset. At most four tasks
should be active at each worker at a time, as otherwise the tasks quickly will
begin to interfer with each other when many GATK tools are run on the same
worker.

### How to build this project
By default SparkCaller is packaged with all the required dependencies (except
Spark and Scala) into a single JAR file. All the dependencies are handled by Maven, and
the package can be built using `mvn clean package`.

#### Dependencies
The following dependencies are required, but not provided inside the JAR:
* spark-core_2.11
* scala-library 2.11.8

### GCAT test results
* [Illumina 100bp pair-ended exome 30x](http://www.bioplanet.com/gcat/reports/8098-jbosisorkp/variant-calls/illumina-100bp-pe-exome-30x/sparkbwa-sparkcaller/compare-8088-uxcggxlhzc-7997-cqiyxsnvoq/group-read-depth)

### Benchmarking data
The large dataset is the Illumina 100bp pair-ended exome 150x GCAT dataset. The
small dataset refers to the Ion Torrent 225bp single-ended exome 30x GCAT
dataset. These datasets can be obtained [here](http://www.bioplanet.com/gcat)
by clicking 'Start Test' and creating a test.

Each data point is an average of three runs using the same settings.

![HaplotypeCaller benchmarking large dataset](img/haplotypecaller_large.png "")
![HaplotypeCaller benchmarking small dataset](img/haplotypecaller_small.png "")

![BQSR benchmarking large dataset](img/bqsr_large.png "")
![BQSR benchmarking small dataset](img/bqsr_small.png "")

![Indel realignment benchmarking large dataset](img/indel_realignment_large.png "")
![indel realignment benchmarking small dataset](img/indel_realignment_small.png "")

