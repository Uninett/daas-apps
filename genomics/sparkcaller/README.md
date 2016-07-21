### How to build this project
By default SparkCaller is packaged with all the required dependencies (except
Spark) into a single JAR file. All the dependencies are handled by Maven, and
the package can be built using `mvn clean package`.

A version of the JAR can be found [here](https://f.128.no/).

### Usage
```
java -jar sparkcaller-1.0.jar                    \
-CPN <Cores per node>                            \
-O <Output folder>                               \
-C <Path to config file>                         \
-R <Path to reference file>                      \
-I <Path to the folder containing the SAM files> \
-S <Path to known sites>
```

Ex.
```
java -jar sparkcaller-1.0.jar                    \
-CPN 4                                           \
-O /data/hdfs/sparkcaller/                       \
-C sparkcaller.properties                        \
-R /data/hdfs/1000genomes/hg19/ucsc.hg19.fasta   \
-I /data/hdfs/sparkcaller/sams/                  \
-S /data/hdfs/1000genomes/dbsnp/human/dbsnp_138.hg19.vcf
```

### Configuration per tool
It is possible to pass abitary arguments to each tool in the GATK toolkit.
The input, reference, and output arguments are set automatically (KnownSites is
also set in BQSR).

The config uses the normal Java property format. The VariantRecalibrator can
for example be configured in the following way:
```
SNPVariantRecalibrator = -resource:hapmap,known=false,training=true,truth=true,prior=15.0 PATH_TO/hapmap_3.3.hg19.sites.vcf \
                         -resource:omni,known=false,training=true,truth=false,prior=12.0 PATH_TO/1000G_omni2.5.hg19.sites.vcf \
                         -resource:1000G,known=false,training=true,truth=false,prior=10.0 PATH_TO/1000G_phase1.snps.high_confidence.hg19.sites.vcf \
                         -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 PATH_TO/dbsnp_138.hg19.vcf \
                         --variant_index_type LINEAR --variant_index_parameter 128000 \
                         -an MQRankSum \
                         -an ReadPosRankSum \
                         -an BaseQRankSum \
                         --maxGaussians 4 \
                         -minNumBad 7500 \

```

In order to run both INDEL and SNP recalibrator, add a similar line to what is
added for SNPVariantRecalibrator (except for INDELVariantRecalibrator).
