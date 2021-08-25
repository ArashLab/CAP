# Tutorials for Cohort Analysis Platform (CAP)

**Examples are tested with version 0.1.11 on pypi**

The [examples](examples) folder includes three subsets of [1000-Genome project phase 3 dataset](https://www.internationalgenome.org/data/) all of which contains the full set of 2504 samples in the original dataset. However variants are random subset of different size as below:
* *1kg.micro.vcf.bgz*: 193 variant
* *1kg.tiny.vcf.bgz*: 4053 variant
* *1kg.small.vcf.bgz*: 40408 variant

A simulated phenotypic file (*1kg.pheno.tsv*) is aslo included that present Type2 Diabetes (T2D) and Body Mass Index (BMI) for 100-genome samples along with other real phenotipic data. 

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

The following is a minimal workload that reads a VCF file and write it to a Hail MatrixTable format (with `.mt` extension). The corresponding cap function to do so is **ImportGenotype**. This function requires input and output genotype file (input and output). Here is the content of workload file. Note that `IGTVCF` is a unique id for this stage.
Note that **ImportGenotype** can read data from different format (currently VCF and Plink Bfiles are supported). It accepts parameters (`arg`) to be passed to the relevant Hail function. See Documentaition for more details. 

```yaml
order:
    - IGTVCF
stages:
    IGTVCF:
        spec:
            function: ImportGenotype
        io:
            input:
                direction: input
                path: 1kg.micro.vcf.bgz
            output:
                direction: output
                path: 1kg.ma.mt
```

To execute this workload run the `main` module in the `cap` package with the example workload file.

```bash
python -m cap.main --workload Example_01.yaml
```

Notes:
* If you installed cap in a virtual environemnt, you should activate that environment before runing this above command.
* You must be inside the `temp` directory created above.

The sucessfull compeletion of this workload creates the following files in the current directory.
* *1kg.ma.mt*: The output (Hail MatrixTable is stored in a directory). Since there are multi-alleic site in this dataset we add `ma` to the file name.
* *hail-something.log*: The log file generated by Hail
* *cap.something.log.tsv*: The log file generated by CAP
Note that both log files include date-time element as well as a random string (instead of `something`) to avoid overwriteing older log files.
The CAP log file contains 5 columns as below:
* Date-Time of the log
* Log level
* Current stage when log is generated (if log is not relevant to any stage the value of None is used)
* Corresponding function
* Log message

If you look at the content of the the `Example_01.yaml` file after execution of the workload, you will see information is added to this file.
Some of this information are default values which are added when the workload file is loaded (i.e. `pathType` in the `input`) and some others are information that is dynamically added (i.e. `count` in the `output`).
The most important one for you to know is the `spec.status` field with is set to `Compeleted`. If you execute the workload once more, it does not execute this stage as it is already marked as `Compeleted`. This feature is quiet handy for the case you have many stages in your workload and it fails at some point. Once you fix the issue, you can execute the workload and make sure only incompleted stages are executed. 

```yaml
order:
- IGTVCF
stages:
    IGTVCF:
        io:
            input:
                compression: bgz
                direction: input
                format: vcf
                path: 1kg.micro.vcf.bgz
                pathType: file
            output:
                compression: None
                count:
                    samples: 2504
                    variants: 193
                data: <hail.matrixtable.MatrixTable object at 0x7fa8515552b0>
                direction: output
                format: mt
                isAlive: true
                numPartitions: 4
                path: 1kg.ma.mt
                pathType: file
                toBeCached: true
                toBeCounted: true
        spec:
            endTime: '2021-06-24 11:30:05.825676'
            execTime: '0:00:05.114684'
            function: ImportGenotype
            id: IGTVCF
            startTime: '2021-06-24 11:30:00.710992'
            status: Completed
```

In the successfull compeletion of the workload the following is printed in the terminal. We break this into part and explain them.
```
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
LOGGING: writing to hail-20210624-1129-0.2.70-5bb98953a4a7.log
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	[('spark.hadoop.mapreduce.input.fileinputformat.split.minsize', '0'), ('spark.driver.host', '192.168.1.193'), ('spark.hadoop.io.compression.codecs', 'org.apache.hadoop.io.compress.DefaultCodec,is.hail.io.compress.BGzipCodec,is.hail.io.compress.BGzipCodecTbi,org.apache.hadoop.io.compress.GzipCodec'), ('spark.ui.showConsoleProgress', 'false'), ('spark.app.startTime', '1624498198227'), ('spark.executor.id', 'driver'), ('spark.logConf', 'true'), ('spark.kryo.registrator', 'is.hail.kryo.HailKryoRegistrator'), ('spark.driver.port', '61401'), ('spark.app.id', 'local-1624498199213'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.kryoserializer.buffer.max', '1g'), ('spark.driver.maxResultSize', '0'), ('spark.executor.extraClassPath', './hail-all-spark.jar'), ('spark.master', 'local[*]'), ('spark.submit.pyFiles', ''), ('spark.submit.deployMode', 'client'), ('spark.app.name', 'Hail'), ('spark.driver.extraClassPath', 'python3.9/site-packages/hail/backend/hail-all-spark.jar')]
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
IGTVCF	CheckStage	Stage is Checked
IGTVCF	ExecuteStage	Started
2021-06-24 11:30:02 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:03 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:03 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:05 Hail: INFO: wrote matrix table with 193 rows and 2504 columns in 4 partitions to 1kg.ma.mt
    Total size: 41.45 KiB
    * Rows/entries: 31.58 KiB
    * Columns: 9.85 KiB
    * Globals: 11.00 B
    * Smallest partition: 42 rows (4.95 KiB)
    * Largest partition:  52 rows (12.08 KiB)
IGTVCF	ExecuteStage	Completed in 0:00:05.114684
```

The following message shows where CAP is going to write its log
```
*** logger is initialised to write to cap.2021-06-24 11:29:53.034782.K1398THY.log.tsv
```

The workload file is loaded and all stages are checked for errors. In this case, we only have one stage to be checked
```
IGTVCF	CheckStage	Stage is Checked
```

Some Spark stuff.
```
2021-06-24 11:29:57 WARN  NativeCodeLoader:60 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2021-06-24 11:29:58 WARN  Hail:43 - This Hail JAR was compiled for Spark 3.1.1, running with Spark 3.1.2.
  Compatibility is not guaranteed.
Running on Apache Spark version 3.1.2
SparkUI available at http://192.168.1.193:4040
```

Hail initialization and where hail log file is stored
```
Welcome to
     __  __     <>__
    / /_/ /__  __/ /
   / __  / _ `/ / /
  /_/ /_/\_,_/_/_/   version 0.2.70-5bb98953a4a7
LOGGING: writing to hail-20210624-1129-0.2.70-5bb98953a4a7.log
```

We print out the spark configuration used in the process and highlight it with many + signs.
Please note the `('spark.master', 'local[*]')` and `('spark.submit.deployMode', 'client')`
```
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	[('spark.hadoop.mapreduce.input.fileinputformat.split.minsize', '0'), ('spark.driver.host', '192.168.1.193'), ('spark.hadoop.io.compression.codecs', 'org.apache.hadoop.io.compress.DefaultCodec,is.hail.io.compress.BGzipCodec,is.hail.io.compress.BGzipCodecTbi,org.apache.hadoop.io.compress.GzipCodec'), ('spark.ui.showConsoleProgress', 'false'), ('spark.app.startTime', '1624498198227'), ('spark.executor.id', 'driver'), ('spark.logConf', 'true'), ('spark.kryo.registrator', 'is.hail.kryo.HailKryoRegistrator'), ('spark.driver.port', '61401'), ('spark.app.id', 'local-1624498199213'), ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), ('spark.kryoserializer.buffer.max', '1g'), ('spark.driver.maxResultSize', '0'), ('spark.executor.extraClassPath', './hail-all-spark.jar'), ('spark.master', 'local[*]'), ('spark.submit.pyFiles', ''), ('spark.submit.deployMode', 'client'), ('spark.app.name', 'Hail'), ('spark.driver.extraClassPath', 'python3.9/site-packages/hail/backend/hail-all-spark.jar')]
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
None	__init__	+++++++++++++++++++++++++++++++
```

Each stage is being check once again right before execution. Everything between `IGTVCF	ExecuteStage	Started` and `IGTVCF	ExecuteStage	Completed in 0:00:05.114684` are messages printed by hail.
```
IGTVCF	CheckStage	Stage is Checked
IGTVCF	ExecuteStage	Started
2021-06-24 11:30:02 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:03 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:03 Hail: INFO: Coerced sorted dataset
2021-06-24 11:30:05 Hail: INFO: wrote matrix table with 193 rows and 2504 columns in 4 partitions to 1kg.ma.mt
    Total size: 41.45 KiB
    * Rows/entries: 31.58 KiB
    * Columns: 9.85 KiB
    * Globals: 11.00 B
    * Smallest partition: 42 rows (4.95 KiB)
    * Largest partition:  52 rows (12.08 KiB)
IGTVCF	ExecuteStage	Completed in 0:00:05.114684
```


## Example 2 (Break Multi-Allelic Loci)

The **SplitMulti** function in CAP help to break multi-allleic site to multiple bi-alleleic site. In this example, we pass the `withHTS` argument to the function too. `ba` in the output file name referes to bi-allelic.


```yaml
order:
    - BRKMA
stages:
    BRKMA:
        spec:
            function: SplitMulti
        arg:
            withHTS: true
        io:
            input:
                direction: input
                path: 1kg.ma.mt
            output:
                direction: output
                path: 1kg.ba.mt
```

The updated workload file after this example show that the number of loci increased by 2 (from 193 to 195).

```yaml
stages:
    BRKMA:
        io:
            input:
                count:
                    variants: 193
            output:
                count:
                    variants: 195
```

## Example 3 (Multiple Stages in One workload)

The following example shows how to put two stages in one workload. Note that the order of stage ids in the `order` field is important (they are executed in the same order) but you don't need to order the definition of stages. Also we define `mtPathMa` to avoid replication of the file name across our workload file.

Note that `1kg.ma.mt` is the output of `IGTVCF` and the input to the `BRKMA`. When you run this workflow, CAP keeps the `1kg.ma.mt` alive in the memory to move through stages.

```yaml
config:
    mtPathMa: &mtPathMa 1kg.ma.mt
order:
    - IGTVCF
    - BRKMA
stages:
    IGTVCF:
        spec:
            function: ImportGenotype
        io:
            input:
                direction: input
                path: 1kg.micro.vcf.bgz
            output:
                direction: output
                path: *mtPathMa
    BRKMA:
        spec:
            function: SplitMulti
        arg:
            withHTS: true
        io:
            input:
                direction: input
                path: *mtPathMa
            output:
                direction: output
                path: 1kg.ba.mt
```

If you execute this workload after you execute example 1 and 2, you will get and exception error message as follow. When CAP checks stages, it checks that output files does not exist on the system to prevent overwriting. Since example 1 already produced `1kg.ma.mt` you will get this error.

```
Exception: IGTVCF	Checkio	<< io: output >> Output path (or plink bfile prefix) /Users/arashbayat/newCAP/CAP/temp/1kg.ma.mt already exist in the file system
```

If you insist to run this workload you need to delete output files first
```bash
rm -rf *.mt
python -m cap.main --workload Example_03.yaml
```

## Example 4 (Add Numeric Ids)

Most of CAP functions requiers a uniqe and numeric sample and variant ids (`sampleId` and `variantId`). These ids are used to connect all tables together. Also, depending on the source file where genotypes are loaded, there could be annotations for samples and variants loaded into the matrix table. The **AddId** function in CAP adds the numeric Ids and also extract all sample and variant annotations and export them in separate HailTbale (with `.ht` extension).

```yaml
order:
    - ADDID
stages:
    ADDID:
        spec:
            function: AddId
        io:
            input:
                direction: input
                path: 1kg.ba.mt
            output:
                direction: output
                path: 1kg.mt
            outCol:
                direction: output
                path: 1kg.samples.ht
            outRow:
                direction: output
                path: 1kg.variants.ht
```

## Example 5 (Vriant Annotations to TSV)

1000-Genome VCF file which we used in our example includes several variant annotations. The **AddId** function export them into `1kg.variants.ht` files and now we would like to show you how to export these information into a TSV file. 

```yaml
order:
    - TSVVAR
stages:
    TSVVAR:
        spec:
            function: ToText
        arg:
            header: true
            delimiter: "\t"
        io:
            inHt:
                direction: input
                path: 1kg.variants.ht
            outText:
                direction: output
                path: 1kg.variants.tsv
```

Here is a few first line from `1kg.variants.tsv`:

```
locus.contig	locus.position	rsid	qual	a_index	was_split	variantId	alleles.1	alleles.2
1	20788154	rs557428684	1.0000e+02	1	false	1	A	G
1	36434303	rs574016683	1.0000e+02	1	false	2	T	C
1	36905328	rs149158928	1.0000e+02	1	false	3	A	G
1	40566531	rs538135875	1.0000e+02	1	false	4	T	C
1	61171864	rs544695542	1.0000e+02	1	false	5	A	C
1	70114929	rs190757654	1.0000e+02	1	false	6	A	T
1	86500526	rs375700092	1.0000e+02	1	false	7	T	G
1	93992857	rs184594653	1.0000e+02	1	false	8	C	T
1	97156615	rs528557531	1.0000e+02	1	false	9	A	G
```
## Example 6 (All together)

This example include the following stages:
* `IGTVCF`: Import GenoType from VCF file
* `BRKMA`: Break multi-allelic loci
* `ADDID`: Add numeric ids
* `TSVSAM`: Export sample information in TSV format
* `TSVVAR`: Export variant information in TSV format
* `EGTVCF`: Export Genotype (output of `ADDID`) in VCF format
* `EGTPLINK`: Export Genotype (output of `ADDID`) in plink2 bfile format
* `IPT`: Import Phenotype form TSV file. This stage also requiers `1kg.samples.ht` that is the output of `ADDID` to link phenotype with the numerical sample ids. 
* `TSVPHENO`: Export Phenotype in TSV format. This is as same as input phenotype but also include the numerical sample ids
* `PCA`: Perform Principle Component Analysis (PCA) and compute scores and loading. It also output list of variants included in the calculation. The PCA function allows to sub-sampleing data.
* `TSVPCASCORE`: Export PCA scores in TSV format
* `TSVPCAVAR`: Export list of variant used in PCA calculation in TSV format.
* `SQC`: Calculate Quality Control (QC) metrics for samples
* `VQC`: Calculate Quality Control (QC) metrics for variants
* `TSVSQC`: Export sample QC in TSV format
* `TSVVQC`: Export variant QC in TSV format

To run this example start from a fresh copy of the [examples](examples) folder. 
A copy of the expected output form this example is provided in the [examples](examples) folder (see [Example_06_Output.tar.gz](examples/Example_06_Output.tar.gz))

## Example 7 (Write to MySQL)
You can write data into MySQL tables too. You need access to a MySQL database and JDBC conector for spark.

Once you download the JDBC into your system. You can add `jar` file to your spark submit command using the following trick. Remember to replace the `full_path` with and if you use a different version of it you need to change the version in the following command. 

```bash
export PYSPARK_SUBMIT_ARGS='--jars /full_path/mysql-connector-java-8.0.22/mysql-connector-java-8.0.22.jar pyspark-shell'
```

note that in the following example you should replace the following to define your access to MySQL server.
* \_\_YOUR_MYSQL_PASSWORD__
* \_\_YOUR_MYSQL_ADDRESS__
* \_\_YOUR_MYSQL_DATABASE__
* \_\_YOUR_MYSQL_USER__

```yaml
config:
    mySqlConfig: &mySqlConfig
        driver: com.mysql.jdbc.Driver
        password: __YOUR_MYSQL_PASSWORD__
        url: jdbc:mysql://__YOUR_MYSQL_ADDRESS__/__YOUR_MYSQL_DATABASE__?useLegacyDatetimeCode=false&serverTimezone=UTC
        user: __YOUR_MYSQL_USER__
order:
    - SQLVAR
stages:
    SQLVAR:
        spec:
            function: ToMySql
        arg:
            mySqlConfig:
                <<: *mySqlConfig
                dbtable: variant # This is the name of table to be created in your mysql database.
        io:
            inHt:
                direction: input
                path: 1kg.variant.ht
```

## Example 8 (VEP Annotation)
To run this example you need to have VEP-CLI installed on your system. We use [VEP Docker container](https://hub.docker.com/r/ensemblorg/ensembl-vep) for that as installing VEP-CLI is pretty complicated.
In our example we use VEP CACHE file as the annotation database. Its less than 20GB and you can download it from VEP website. You can change the example to use a different annotation source. 

VEP annotation occures in 3 stages which are included in this example:
**EGTVEP**: This stage export genotype in VCF format with forVep argument set to true. This result in droping all sample information and keeping only the locus and allele informations. Also it reaplces the rsid field with the numerical variant id to be able to join the VEP result with the rest of variant related tables produced by CAP.
Also the genotypes are exported in parallel. That means each partition of the matrix table is written in a separate VCF file. The output of this step is named `1kg.forVep.vcf.bgz`, however it is not a file but a directory with the following content. Since the input MatrixTable had 4 partitions, the data is divided in 4 parts. Note that these parts are actually `vcf.bgz` eventhough `vcf` is missing in the file name.
You can process each part independently and in parallel. Note that VEP annotation is a time-consuming processe and the CAP VEP parser runs sequentianlly. Idealy you should have a job scheduler such as Sun Grid Engin (SGE) where you can submit jobs and jobs running in parallel. In this example, we run 4 jobs sequentinally (see the next stage).

```
_SUCCESS
header.bgz
part-00000.bgz
part-00001.bgz
part-00002.bgz
part-00003.bgz
```

**VEPANN**:
This stage uses **VepAnnotation** function in cap to process each VCF part independently. First it uses VEP to produce variant annotation in JSON format, then it run CAP VEP parser to parse the JSON format and produce set of TSV files used in the next stage. Note that here we process VCF part sequentinally. However, If you have a job scheduler such as Sun Grid Engin (SGE) you can submit them as jobs and make sure they are executed in parallel. 

This stage uses a shell script ([cvs-norm.sh](examples/vep/cvs-norm.sh)) to call VEP and CAP VEP Parser. This shell script uses two other shell scrtip ([cvs.sh](examples/vep/cvs.sh) and [vep.sh](examples/vep/vep.sh)) all of which are included in [examples/vep](examples/vep/) directory in this reposetory. We use python subprocess module to run [cvs-norm.sh](examples/vep/cvs-norm.sh) and the command line argument can be found in the stage argument as below. Note that values wrapped with double underscore are replaced by the CAP when executing the script.

The output is a set of TSV file per VCF parts. For more information refers to our [documentation page on GitHub](https://github.com/ArashLab/CAP/blob/main/DOCUMENTATION.md)

```yaml
VEPANN:
        spec:
            function: VepAnnotation
        arg:
            vepCli: 
                - bash
                - vep/cvs-norm.sh
                - __IN_VCF__
                - __OUT_JSON__
                - __OUT_TSV__
                - NoJobId
                - __OUT_JOB__
                - vep
        io:
            inVar:
                compression: bgz
                direction: input
                format: vcf
                path: *vepVcfPath
                pathType: parallel
            outData:
                direction: output
                format: tsv # TBF
                path: *vepOutPath
```

**VEPLT**: This stage merge the TSV file produced for different parts, load then to HailTable and process them to generate a few more tables. and then output the folowing tables. For more information on this stage refer to our [documentation page on GitHub](https://github.com/ArashLab/CAP/blob/main/DOCUMENTATION.md)

* var: Main variant annotations
* clvar: Co-Located variants linked to each variants
* freq: Frequencies for each Co-Located variatn
* conseqVar: Linking variants to consequences (it is a many to many relationship)
* conseq: Consequences
* conseqTerms: Linking Consequences to Consequence terms (each consequence could have multiple tersm)
* varTerms: Linking Variants to Consequence terms (each variant could have multiple consequence and each consequence could have multiple tersm)

The last table is for performace reason only as it could be obtained by joining other tables.


There are other stages in this example workload to export the resulting VEP annotation tables into TSV format. The command below reveals the first few line of those output.

```bash
head -n 3 *.tsv
```

That looks like:

```
==> 1kg.vep.var.tsv <==
strand	variant_class	end	most_severe_consequence	start	allele_string	assembly_name	seq_region_name	varId
1	SNV	91093367	intergenic_variant	91093367	C/T	GRCh37	12	145
1	SNV	78880078	intron_variant	78880078	T/C	GRCh37	13	149

==> 1kg.vep.clvar.tsv <==
allele_string	start	minor_allele_freq	strand	seq_region_name	id	minor_allele	end	varId	clVarId	fileNumber	somatic	phenotype_or_disease	pubmed
T/C	36434303	0.0012	1	1	rs574016683	C	36434303	2	1	0	NA	NA	NA
A/T	70114929	0.001	1	1	rs190757654	T	70114929	6	5	0	NA	NA	NA

==> 1kg.vep.freq.tsv <==
afr	sas	amr	eur	eas	varId	clvarId	allele	gnomad_oth	gnomad_nfe	aa	gnomad	gnomad_afr	gnomad_sas	gnomad_asj	egnomad_fin	gnomad_eas	gnomad_amr
0.0000e+00	0.0000e+00	1.4000e-03	0.0000e+00	0.0000e+00	145	1	T									
0.0000e+00	7.2000e-03	0.0000e+00	0.0000e+00	0.0000e+00	149	5	C									

==> 1kg.vep.conseqVar.tsv <==
conseqId	varId
0	173
1	173

==> 1kg.vep.conseq.tsv <==
variant_allele	impact	feature	transcript_id	strand	ccds	intron	swissprot	gene_symbol	gene_symbol_source	trembl	given_ref	uniparc	protein_id	biotype	source	used_ref	hgvsc	hgnc_id	gene_id	canonical	flags	distance	gene_pheno	bam_edit	regulatory_feature_id	exon	cdna_end	cdna_start	mirna	transcription_factors	motif_name	high_inf_pos	motif_feature_id	motif_score_change	motif_pos	conseqId
-	MODIFIER	transcript	ENST00000583426	1.0				RP11-464D20.6	Clone_based_vega_gene		TTTTG			sense_intronic	Ensembl	TTTTG			ENSG00000264546	1.0		4260.0							NA				0
-	MODIFIER	transcript	ENST00000583843	1.0				TLK2	HGNC	['J3QQN4_HUMAN']	TTTTG	['UPI000268B2E6']	ENSP00000463814	protein_coding	Ensembl	TTTTG		11842.0	ENSG00000146872		['cds_end_NF']	4042.0	1.0						N1

==> 1kg.vep.conseqTerms.tsv <==
conseqId	consequence_terms
0	downstream_gene_variant
1	downstream_gene_variant


==> 1kg.vep.varTerms.tsv <==
varId	consequence_terms
1	intergenic_variant
1	intergenic_variant
```

A copy of the expected output form this example is provided in the [examples](examples) folder (see [Example_08_Output.tar.gz](examples/Example_08_Output.tar.gz))