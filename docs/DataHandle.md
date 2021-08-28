used (as input) or produced (as output) in a job.
The DataHandle is produced once but could be used by multiple analysis steps.
Rather describing specification of the data in each of the connected jobs, the specification is described once in the DataHandle and all the connected jobs point to the DataHandle.


# DataHandle
A DataHandle is the specification of the data. What do we mean by data and its specification.
Data could be anything, a single integer number or a giant table where each entry is a complex structure.
Example of data includes a VCF file, a phenotype file and a Python list stored in the momory.
Data can be permanent (like files) or temporary (like a python object in memory).
Permanent data is stored in different storage such as local disk, tape storage, NAS and HDFS as well as cloud storage such as S3 and Dropbox.
For simplicity we use the term **Disk** to refer to all those
Permanent data could be in form of file(s), databases or something else (just in case).
Examples are
- A JSON file (single file)
- 1000 Genomes Project VCF divided by choromosems (multiple files yet we would like to have it as one peice of data)
- A MySQL table contains computed PCA scores for samples (database storage)


Some analysis requiers data to be loaded into the memory prior to the execution.
The format data is stored in disk and the format it is stored in the memory are not the same.
Also there is not a one-to-one binding of these formats.
Also the data can be compressed or uncompressed in both disk and memory.
For example, tabular data can be stored using the following formats:
- Disk:
    - csv (comma-seprated values)
    - tsv (tab-seprated values)
    - parquet
    - ht (Hail)
    - MySQL table
- Memory (Python, PySpark):
    - Pandas Dataframe
    - Numpy 2D array
    - Python list of dicts
    - JSON string
    - Spark Dataframe
    - Spark RDD
    - Hail Table


    
**A DataHandle is the disk and the memory specification of the data.**.
There are many fields to describe a DataHandle.
However, you don't need to write the entire structure for each new DataHandle.
In most cases, all you need is to put the path to the file and CAP infers the rest of the field (see [Infer DataHandle](InferDataHandle.md)).
This example define a DataHan :
```yaml
MyDataHandle:
    disk:
        path: hdfs:///users/me/input.vcf.bgz
```

CAP infers the following information given the above definition:
```yaml
MyDataHandle:
    disk:
        path: hdfs:///users/me/input.vcf.bgz
        isProduced: True
```


Notes:
- Temporary DataHandle does not have a disk specification.
- A DataHandle may not have memory specification unless an internal operation requiere data to be loaded into memory.
- Temporary DataHandle cannot be recovered if the CAP process is failed.
- 
A temporary DataHandle is not requierd to be defined as CAP will add its definitions when it is first used in one of the analysis steps.

There is a flags in both disk and memory section to determind if the data is produced.
In fact, 

CAP source code isolates and automates the process of loading data from disk to memory (when needed) and dumping data from memory to disk (when produced).
This improve readability and simplicity of the source code.

