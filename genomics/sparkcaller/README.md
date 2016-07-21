### How to build this project
By default SparkCaller is packaged with all the required dependencies (except
Spark) into a single JAR file. All the dependencies are handled by Maven, and
the package can be built using `mvn clean package`.

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
