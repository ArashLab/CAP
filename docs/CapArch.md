# CAP Architecture in Short

CAP can be seen as a simple workflow manager with its own terminology:
- `Workflow` refers to a file that contains detailed description of the analysis.
- `Operation` is the implementation of one analysis step. Limited number of operations are implmented currently. More operations will be introduced with each new version we release. 
- `Job` is the specification of a step in the analysis. Each job is linked to an operation. A job specify input, output and parameters used to execute the operation. 
- `DataHandle` is the specification of an input or an output of a job.
- `Flow` is the order of jobs to be executed in the analysis. Currently CAP only support a simple linear execution of the jobs and does not automatically check the data dependency between the jobs.
- `Config` is the configuration of the analysis (i.e. default values)
- `Runtime` is the runtime information about execution of the analysis (i.e. when every job is started to finished, what version of eahc tool is used in the analysis)

CAP reads the workflow and constantly updates its runtime as the analysis is progressed.
To minimise errors in the execution, CAP checks the workflow schema at different stages to make sure the workflow contains valid information.
If a CAP process is re-executed after a failour, it skips all the steps which are completed and continue from where it fails.

As described above, CAP is a basic workflow manager. **However, our main contributions listed below are beyond a workflow manager:**
- CAP introduce the syntax that best suits describing genomic cohort analysis. 
- CAP offers a set of pre-coded and configurable operations used in the genomic cohort analysis. These operations allow seamless communication between various genomic software such as Hail, VEP and many more.
- CAP executes multiple steps in one process. So the data flows between analysis steps in the memory (where possible). Workflow managers orchestrate independent steps executed in independent processes. Note that writing and reading large genomic dataset to/from disk takes a lot of time. This is specially unacceptable for a chain of small operations where the intermediate data is not of interest.
- CAP parallelises the workload over multiple compute nodes.

## Workflow File
The workflow file describes the analysis in *YAML* or *JSON* format. These formats are simple, human-readable and widely used to store configuration data. If you don't know *YAML*, don't worry. You can learn all you need to know in a 4-minute tutorial [here](https://youtu.be/0fbnyS_lHW4). Also, the [Geting Started](GetingStarted.md) includes a few examples that can help you understand these formats.

CAP requires specific information to be presented in the workflow. The details are provided in [Describe Your Analysis](docs/DescribeAnalysis.md).

The workflow file includes information about the following:
### DataHandles:
A DataHandle is the specification of the data used (as input) or produced (as output) in the analysis step. 
The DataHandle is produced once but could be used by multiple analysis steps.
Rather describing specification of the data in each of those step, all DataHandles are described once only in `DataHandles` section of the workflow.
Input(s) and output(s) of analysis steps can only point to a DataHandles.

Usually data is stored on permanent volumes (i.e local disk, tape storage, NAS, HDFS, Cloud Storage, etc), most likely in (a) file(s), but it could live in a database or other shape too.
For simplicity, we use the term `Disk` to refer to a permanent storage (same in the CAP source code).
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
A DataHandle is the `disk` and the `memory` specification of the data.
Some DataHandles are temporary that means they don't have any disk specification.
Temporary date live only in the memory and cannot be recovered if the CAP process is failed.
A temporary DataHandle is not requierd to be defined as CAP will add its definitions when it is first used in one of the analysis steps.

There is a flags in both disk and memory section to determind if the data is produced.
In fact, a DataHandle is a complex object with many fields.
However, you don't need to write the entire structure for each new DataHandle.
In most cases, all you need is to put the path to the file and CAP infers the rest of the field.

CAP source code isolates and automates the process of loading data from disk to memory (when needed) and dumping data from memory to disk (when produced).
This improve readability and simplicity of the source code.

Here is an example of a DataHandle definition:
```yaml
DataHandels:
    MyDataHandle:
        disk:
            path: hdfs:///users/me/input.vcf.bgz
            isProduced: True
```

CAP infers the following information given the above definition:
```yaml
MyDataHandle:
    disk:
        path: hdfs:///users/me/input.vcf.bgz
        isProduced: True
        BlaBla: Bla
```

### Jobs:
A Job is the specification of one analysis step. Only jobs which are listed in the *flow* section are executed by CAP. Here is an example of the job:

```yaml
Jobs:
    disk:
        path: hdfs:///users/me/input.vcf.bgz
        isProduced: True
        BlaBla: Bla
```

This job is linked to the most basic operation in CAP called `BYPASS`. As its name suggest it does nothing but to bypass the only input to the only output. Yet there are alot you can do with this nothing. 


The input(s) and output(s) of operations are pointers to DataHandles. Each operation in the workflow file is linked to a pre-coded operation implmented in CAP. 


