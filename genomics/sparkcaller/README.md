### How to build this project
By default SparkCaller is packaged with all the required dependencies (except
Spark and Scala) into a single JAR file. All the dependencies are handled by Maven, and
the package can be built using `mvn clean package`.

### How it works
![SparkCaller pipeline](img/sparkcaller_pipeline.png "How the pipeline is run
using the SparkCaller")

### Usage
```
spark-submit                                      \
--class com.github.sparkcaller.SparkCaller        \
--executor-memory <RAM per executor>              \
--driver-memory  <driver RAM>                     \
sparkcaller-1.0.jar                               \
-CPN <Cores per node>                             \
-O <Output folder>                                \
-C <Path to config file>                          \
-R <Path to reference file>                       \
-I <Path to the folder containing the SAM files>  \
-S <Path to known sites>
-F <The fileformat which to use as input (BAM|SAM|VCF)
```

Ex.
```
spark-submit                                      \
--class com.github.sparkcaller.SparkCaller        \
--executor-memory 16G                             \
--driver-memory  6G                               \
sparkcaller-1.0.jar                               \
-CPN 4                                            \
-O /data/hdfs/sparkcaller/                        \
-C sparkcaller.properties                         \
-R /data/hdfs/1000genomes/hg19/ucsc.hg19.fasta    \
-I /data/hdfs/sparkcaller/sams/                   \
-S /data/hdfs/1000genomes/dbsnp/human/dbsnp_138.hg19.vcf
-F sam
```

You can also use the sh script `submit-sparkcaller.sh` to start the job.
This script passes all arguments directly to the sparkcaller. Edit the file to
change the default Spark options.
The script can be used in the following way:

```
./submit-sparkcaller.sh                           \
-CPN 4                                            \
-O /data/hdfs/sparkcaller/                        \
-C sparkcaller.properties                         \
-R /data/hdfs/1000genomes/hg19/ucsc.hg19.fasta    \
-I /data/hdfs/sparkcaller/sams/                   \
-S /data/hdfs/1000genomes/dbsnp/human/dbsnp_138.hg19.vcf
-F sam
```

### Configuration per tool
It is possible to pass arbitrary arguments to each tool in the GATK toolkit.
The input, reference, and output arguments are set automatically (KnownSites is
also set in BQSR).

Keep in mind that the tools which are not present in the configuration file
will be skipped. 

The name which GATK uses for the tool is used as the key. The following keys
are valid:

* AddOrReplaceReadGroups
* MarkDuplicates
* RealignerTargetCreator
* IndelRealigner
* BaseRecalibrator
* PrintReads
* HaplotypeCaller

#### Arguments already specified by SparkCaller
* RealignerTargetCreator:
	* -nt
	* -R
	* -T
	* -I
	* -o
* IndelRealigner:
	* -R
	* -targetIntervals
	* -I
	* -o
	* -L
* BaseRecalibrator
	* -R
	* -knownSites
	* -nct
	* -I
	* -o
* PrintReads
	* -R
	* -BQSR
	* -nct
	* -I
	* -o
	* -L
* HaplotypeCaller
	* -R
	* -nct
	* -I
	* -o
	* -L

### GCAT test results
* [Illumini 100 bp pe exome 30x](http://www.bioplanet.com/gcat/reports/8098-jbosisorkp/variant-calls/illumina-100bp-pe-exome-30x/sparkbwa-sparkcaller/compare-8088-uxcggxlhzc-7997-cqiyxsnvoq/group-read-depth)
