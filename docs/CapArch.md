# CAP Architecture in Short

CAP can be seen as a simple workflow manager.
The *Workflow* is a text file that contains the description of the analysis.
CAP has a module that read the workflow file and constantly updates it as the analysis is progressed. To minimise runtime error, CAP performs several checks to make sure the workflow contains valid information before the execution of each step. If a CAP process is failed and re-executed, it skips all the steps which are completed and continue from where it fails.

CAP contributions are beyond being a simple workflow manager. These contributions are:
- CAP offers a set of pre-coded and configurable operations used in the genomic cohort analysis. These operations allow seamless communication between various genomic software such as Hail, VEP and many more.
- CAP executes multiple steps in one process (Data flow between steps in the memory where possible). Workflow managers can orchestrate independent steps executed in independent processes. Note that writing and reading large genomic dataset to/from disk takes a lot of time. This is specially a unacceptable for chain of small operation where the intermediate data is not of interest.
- CAP parallelises the workload over multiple compute nodes.

## Workflow File
The *Workflow* file describes the analysis in *YAML* or *JSON* file format. These formats are simple, human-readable and widely used to store configuration data. If you don't know *YAML*, don't worry. You can learn all you need to know in a 4-minute tutorial [here](https://youtu.be/0fbnyS_lHW4). Also, the [Getting Started](GettingStarted.md) includes a few examples that can help you understand these formats.

CAP requires specific information to be presented in the workflow file. The details are provided in [Describe Your Analysis](docs/DescribeAnalysis.md).

The workflow file includes information about the following:
### DataHandle:
A *DataHandle* is the specification of the data used (as input) or produced (as output) in the analysis. 
The DataHandle is produced once but could be used by multiple analysis steps.
Rather describing specification of data in each of those step, all DataHandles are described in DataHandle section of the workload file.
Input(s) and output(s) of analysis steps can only point to one of the DataHandle.

Usually data is stored on permanent volumes (i.e local disk, tape storage, NAS, HDFS, Cloud Storage, etc), most likely in (a) file(s), but it could live in a database or other shape too.
For simplicity we use the term `disk` to refer to permanent storage (same in the CAP source code).
Some analysis requiers data to be loaded into the memory prior to the execution.
The format data is stored in disk and the format it is stored in the memory are not the same.
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
    - Hail Table
Also the data can be compressed or uncompressed in both disk and memory.
A DataHandle is the disk and the memory specification of the data.
Some DataHandles are temporary that means they don't have any disk specification.
Temporary date live only in the memory and cannot be recovered if the CAP process is failed.

There is a flags in both disk and memory section to determind if the data is produced.
In fact, a DataHandle is a complex object with many fields.
However, you don't need to write the entire structure for each new DataHandle.
In most cases, all you need is to put the path to the file and CAP infers the rest of the field.
A temporary DataHandle is not requierd to be defined as CAP will add its definitions when it is first used in one of the analysis steps.
CAP source code isolates and automates the process of loading data from disk to memory (when needed) and dumping data from memory to disk (when produced).
This improve simplicity of the source code.

### Stages:
specification of each analysis step. The input(s) and output(s) of each step is a pointer to one of the file 


