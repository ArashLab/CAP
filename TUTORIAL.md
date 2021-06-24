# Cohort Analysis Platform (CAP)

The [examples](examples) folder includes three subsets of [1000-Genome project phase 3 dataset](https://www.internationalgenome.org/data/) all of which contains the full set of 2504 samples in the original dataset. However variants are random subset of different size as below:
* *1kg.micro.vcf.bgz*: 193 variant
* *1kg.tiny.vcf.bgz*: 4053 variant
* *1kg.small.vcf.bgz*: 40408 variant

In our [examples](examples) we use the *1kg.micro.vcf.bgz* to make sure everything runs fast but you can use larger subsets too.

The [examples](examples) folder contains a series of numbered example workload in yaml format such as [Example_01.yaml](examples/Example_01.yaml). These workload contains one or a few stages to be executed. The [Example_All.yaml](examples/Example_All.yaml) put all stages toghether.
## Before Running the Examples
It is recomended to copy the [examples](examples) directory into another directory (i.e. temp) as workload files gets updated when you execute them and you loose their initial state.

```bash
git clone https://github.com/ArashLab/CAP.git
cd CAP
cp -r examples temp
cd temp
```

Note that hail is a requierment of cap and pyspark is a reqierment of hail. Thus, when you install cap using pip (as described above) pyspark is also installed on your system. If you haven't had previous spark configuration, the newly installed pyspark execute hail in local mode. Otherwise, you may experience difficulties following these examples.
## Example 1 (Import Genotype from a VCF)‍

The following is a minimal workload that reads a VCF file and write it to a Hail MatrixTable format. The corresponding cap function to do so is **ImportGenotype**. This function requires input and output genotype file (inGT and outGt). Here is the content of workload file. 

```yaml
order:
    - IGTVCF
stages:
    IGTVCF:
        inout:
            inGt:
                direction: input
                path: 1kg.micro.vcf.bgz
            outGt:
                direction: output
                path: 1kg.ma.mt
        spec:
            function: ImportGenotype
```

To execute this workload run the `main` module in the `cap` package with the example workload file.

```bash
python -m cap.main --workload Example_01.yaml
```

Notes:
* If you installed cap in a virtual environemnt, you should activate that environment before runing this above command.
* You must be inside the `temp` directory created above.

The sucessfull compeletion of the 

```bash
*** logger is initialised to write to cap.2021-06-24 11:29:53.034782.K1398THY.log.tsv
IGTVCF	CheckStage	Stage is Checked
2021-06-24 11:29:57 WARN  NativeCodeLoader:60 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2021-06-24 11:29:58 WARN  Hail:43 - This Hail JAR was compiled for Spark 3.1.1, running with Spark 3.1.2.
  Compatibility is not guaranteed.
Running on Apache Spark version 3.1.2
SparkUI available at http://192.168.1.193:4040
Welcome to
     __  __     <>__
    / /_/ /__  __/ /
   / __  / _ `/ / /
  /_/ /_/\_,_/_/_/   version 0.2.70-5bb98953a4a7
LOGGING: writing to /Users/arashbayat/newCAP/CAP/temp/hail-20210624-1129-0.2.70-5bb98953a4a7.log
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	[('spark.hadoop.mapreduce.input.fileinputformat.split.minsize', '0'), ('spark.driver.host', '192.168.1.193'), ('spark.hadoop.io.compression.codecs', 'org.apache.hadoop.io.compress.DefaultCodec,is.hail.io.compress.BGzipCodec,is.hail.io.compress.BGzipCodecTbi,org.apache.hadoop.io.compress.GzipCodec'), ('spark.ui.showConsoleProgress', 'false'), ('spark.app.startTime', '1624498198227'), ('spark.executor.id', 'driver'), ('spark.logConf', 'true'), ('spark.kryo.registrator', 'is.hail.kryo.HailKryoRegistrator'), ('spark.driver.port', '61401'), ('spark.app.id', 'local-1624498199213'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.jars', 'file:///Users/arashbayat/Tools/mysql-connector-java-8.0.22/mysql-connector-java-8.0.22.jar'), ('spark.kryoserializer.buffer.max', '1g'), ('spark.repl.local.jars', 'file:///Users/arashbayat/Tools/mysql-connector-java-8.0.22/mysql-connector-java-8.0.22.jar'), ('spark.app.initial.jar.urls', 'spark://192.168.1.193:61401/jars/mysql-connector-java-8.0.22.jar'), ('spark.driver.maxResultSize', '0'), ('spark.executor.extraClassPath', './hail-all-spark.jar'), ('spark.master', 'local[*]'), ('spark.submit.pyFiles', ''), ('spark.submit.deployMode', 'client'), ('spark.app.name', 'Hail'), ('spark.driver.extraClassPath', '/opt/miniconda3/envs/test02/lib/python3.9/site-packages/hail/backend/hail-all-spark.jar')]
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
IGTVCF	CheckStage	Stage is Checked
IGTVCF	ExecuteStage	Started
2021-06-24 11:30:02 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:03 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:03 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:05 Hail: INFO: wrote matrix table with 193 rows and 2504 columns in 4 partitions to /Users/arashbayat/newCAP/CAP/temp/1kg.ma.mt
    Total size: 41.45 KiB
    * Rows/entries: 31.58 KiB
    * Columns: 9.85 KiB
    * Globals: 11.00 B
    * Smallest partition: 42 rows (4.95 KiB)
    * Largest partition:  52 rows (12.08 KiB)
IGTVCF	ExecuteStage	Completed in 0:00:05.114684
```

## Example 2 (Write Result to TSV)

## Example 3 (Quality Metircs)

## Example 4 (Write to MySQL)

## Example 5 (VEP Annotations)

