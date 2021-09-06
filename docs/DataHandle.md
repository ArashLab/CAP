# DataHandle & DataHandler

Usually different processes could talk to each other 

A **DataHandle** is the specification of the data in the workflow file.
`DataHandler` is the coresponding module in CAP source code. 

## Introduction

Data could be anything, a single integer number or a giant table.
Example of data includes a VCF file, a phenotype file and a Python list stored in the momory.
Data can be `permanent` (like files) or volatile (like a python object in memory).
Permanent data is stored in different storage such as local disk, tape storage, NAS and HDFS as well as cloud storage such as S3 and Dropbox.
For simplicity we use the term **Disk** to refer to all those storages.
Permanent data could be in form of file(s), databases or something else (just in case).
Examples are
- A JSON file (single file)
- 1000 Genomes Project VCF divided by choromosems (multiple files yet we would like to have it as one peice of data)
- A MySQL table contains computed PCA scores for samples (database storage)

Some analysis requiers data to be loaded into the memory prior to the execution.
**A DataHandle provides two interface to work with data: disk interface and memory interface.**.
For simplicity we use disk and memory to refer to disk interface and memory interface respectivly.
The format data is stored in disk and the format it is stored in the memory are not the same.
Also the data can be compressed or uncompressed in both disk and memory.
There isn't a one-to-one binding between these formats.
Each disk format can be loaded into multiple memory format and each memory format can be dumped into multiple disks format
For example, tabular data can be stored using the following formats:
- Disk:
    - csv (comma-seprated values)
    - tsv (tab-seprated values)
    - parquet
    - ht (Hail)
    - MySQL table
    - DynamoDB Table (AWS cloud)
- Memory (Python, PySpark):
    - Pandas Dataframe
    - Numpy 2D array
    - Python list of dicts
    - JSON string
    - Spark Dataframe
    - Spark RDD
    - Hail Table

The figure below explain how the data flows between disk, memory and operation

![Data Flow](../Figures/DataFlow.png)

Notes:
- The disk `isProduced` flag is set to `true` when data is written to disk (M2D or O2D).
- The memory `isProduced` flag is set to `true` when data is loaded from disk to memory (D2M) or written to memory by an operation (O2M).
- When the date is written to memory it remains in the memory untile explicitly deleted from memory (see ???)
- All the memory `isProduced` flags are set to `false` each time CAP process is initated.
- There is no overwrite (write when `isProduced` flag is `true`) to disk or memory unless they are explicitly deleted (see ???)
    - When disk is deleted the coresponding memory is deleted too.
    - Memory can be deleted to save space (if the data is not going to be accessed in a near future.). It will not delete data from disk.
- When read from memory that is not produced yet, the DataHandler immediately (and automatically) load data from disk (D2M).
- When data is read from the disk (D2M or D2O) and disk is not produced yet, CAP throw and exception.
- Temporary DataHandles
    - live in the memory during the execution of workflow.
    - don't have a disk specification.
    - cannot be recovered if the CAP process fails.
    - raise exception if read before produced.
    - don't need to be defined BeforeHand (see ???)
- When the operation writes data to the memory (O2M), the DataHandler immediately (and automatically) dump the data to the disk (if not a temporary DataHandler).
- D2M and M2D (data flow between disk and memory) are only perforemd by the DataHanlder **internally**.
- M2O and O2M (data flow between memory and operation) are **wrapped** by the DataHandler.
- D2O and O2D must be performed such that the DataHandler is being noticed (see ???)


There are many fields to describe a DataHandle.
However, you don't need to write the entire structure for each new DataHandle.
In most cases, all you need is to put the path to the file and CAP infers the rest of the field (see [Infer DataHandle](InferDataHandle.md)).
This example define a DataHandle called `inputVCF` connected to a single VCF file:
```yaml
inputVCF:
    disk:
        path: hdfs:///users/me/input.vcf.bgz
```

CAP infers the following information given the above definition:
```yaml
WillBeUpdate: Later
```

## Specification:
A DataHandle has the following structure:
- `id`: must be as same as the DataHandle name. You may leave this field to be inferred.
- `disk`:
    - `isProduced`: binary
    - `format`: see ???
    - `compression`: currently gzip (gz) and bgzip (bgz) are supported
    - `path`: string or list of string or list of `pathStruct` (see ???)
    - `loadParam`: Parameter used to load data from disk (depends on memory format)
    - `dumpParam`: Parameter used to dump date to disk (depends on memory format)
- `memory`
    - `isProduced`: binary
    - `format`: see ???

### Path
`pathStruct` is the compelete representation of a path.
`disk.path` is always inferred to the list of `pathStruct` (see ???).
Note that data may be stored in multiple files or database tables.
For example, a VCF file could be divided by chromosomes.
To address these situation, multiple paths can be linked to a single DataHandle.
Also a path may contain wildcard characters and translate into multiple paths.

The `pathStruct` contains the following field
- `path`: Path to the file (for sql see ???).
    - During execution of the workflow:
        - `path` is copied to `raw`
        - `path` is replaced by the processed path that
            - is absolute and from the file system root
            - has the file system prefix
            - does not contain any environemntal variable (they are replaced by their actual values)
            - does not contain wildcard characters
- `fileSystem`: see ???
- `format`: see ???
- `compression`: see ???
- `raw` is the copy of the given path before being processed. The raw path can be presented in multiple `pathStruct` (i.e. if there is a star wildcard)


### Supported File Systems:

| FileSystem |  prefix |
|:----------:|:-------:|
|  LocalDisk | file:// |
|    HDFS    | hdfs:// |

### Supported Disk Formats

|        Format        | Extension  |
|:--------------------:|:----------:|
|       HailTable      |     ht     |
|    HailMatrixTable   |     mt     |
|   VariantCallFormat  |     vcf    |
| CommaSeparatedValues |     csv    |
|  TabSeparatedValues  |     tsv    |

### Supported Memory Formats

|      Format      |      Type       |
|:----------------:|:---------------:|
|     HailTable    |      Table      |
|  HailMatrixTable |   MatrixTable   |

### Disk and Memory Compatibility

- VariantCallForamt (.vcf)
    - HailMatrixTable
        - Compression: `gzip` and `bgzip`
        - Can read from list of file
        - Can write into multiple files with parallel option in the `dumpParam`
        - `loadParam`: ???
        - `dumpParam`: ???

- CommaSeparatedValues (.csv) & TabSeparatedValues (.tsv)
    - HailTable
        - Compression: `gzip` and `bgzip`
        - Can read from list of file
        - Can write into multiple files with parallel option in the `dumpParam`
        - `loadParam`: ???
        - `dumpParam`: ???

- HailTable (.mt)
    - HailTable 

- HailMatrixTable (.mt)
    - HailMatrixTable

- SqlTable (database)
    - HailTable: Only dump is implemented
    - `dumpParam`:

