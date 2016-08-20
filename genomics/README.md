### Genomics
This folder contains implementations of genomics related tools.
All of the tools uses [Apache Spark](http://spark.apache.org/) to distribute
the workload.

Currently all of the tools does this by dividing the input into smaller chunks,
and then running the tools on each chunk. By doing so we are able to provide
a familiar interface, while (in most cases) improving the performance of each
tool.

### Tools
* **SparkBWA** is a short read aligner which uses
  [BWA](http://bio-bwa.sourceforge.net/) and Spark to
  perform alignment in a distributed manner.
* **SparkCaller** is used both preprocesses the file obtained from
  SparkBWA and does variant discovery. It uses the [GATK
  toolkit](https://www.google.no/search?q=GATK+bqsr&oq=GATK&aqs=chrome.0.69i59j69i57j69i60l4.431j0j1&sourceid=chrome&ie=UTF-8#safe=off&q=GATK+) to perform the
  analysis.

### Requirements
* Indexed FASTA reference files (as normally required in the GATK pipeline).
* dbSNP files for the reference (the one for HG19 can be found
  here: ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle).
* Known, truth, and training sets to be used when performing
  [VQSR](https://www.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_gatk_tools_walkers_variantrecalibration_VariantRecalibrator.php).
* It is preferred that NFS is used over HDFS, as GATK seemingly has some
  problem when writing to HDFS.

![GATK Workflow](img/spark_bio_workflow.png "Parts of the GATK workflow implemented
using Spark")
