# CAP Architecture in Short

CAP can be seen as a simple workflow manager with its own terminology:
- `workflow` refers to a file that contains detailed description of the analysis.
- `operation` is the implementation of an analysis step. Limited number of *operations* are implmented currently. More *operations* will be introduced with each new version we release.
- `job` is the specification of a step in the analysis. Each job is linked to an *operation*. A job specify input, output and parameters used to execute the operation. 
- `dataHandle` is the specification of data used in the input or output of a *job*.
- `executionPlan` is the order of `job`s to be executed in the analysis. Currently CAP only support a simple linear execution of the jobs and does not automatically check the data dependency between the `job`s.
- `config` is the configuration of the analysis (i.e. default values)
- `runtime` is the runtime information about execution of the analysis (i.e. when every job is started to finished, what version of eahc tool is used in the analysis)

CAP reads the `workflow` and constantly updates its runtime as the analysis is progressed.
To minimise errors in the execution, CAP checks the `workflow` schema at different stages to make sure the `workflow` contains valid information.
If a CAP process is re-executed after a failour, it skips all the steps which are completed and continue from where it fails.

As described above, CAP is a basic workflow manager. **However, our main contributions listed below are beyond a workflow manager:**
- CAP introduce the syntax that best suits describing genomic cohort analysis. 
- CAP offers a set of pre-coded and configurable operations used in the genomic cohort analysis. These operations allow seamless communication between various genomic software such as Hail, VEP and many more.
- CAP executes multiple steps in one process. So the data flows between analysis steps in the memory (where possible). Workflow managers orchestrate independent steps executed in independent processes. Note that writing and reading large genomic dataset to/from disk takes a lot of time. This is specially unacceptable for a chain of small operations where the intermediate data is not of interest.
- CAP parallelises the workload over multiple compute nodes.

## Workflow File
The `workflow` describes the analysis in *YAML* or *JSON* format. These formats are simple, human-readable and widely used to store configuration data. If you don't know *YAML*, don't worry. You can learn all you need to know in a 4-minute tutorial [here](https://youtu.be/0fbnyS_lHW4). Also, the [Geting Started](GetingStarted.md) includes a few examples that can help you understand these formats.

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
```

CAP infers the following information given the above definition:
```yaml
DataHandels:
    MyDataHandle:
        disk:
            path: hdfs:///users/me/input.vcf.bgz
            isProduced: True
```

### Jobs:
A Job is the specification of one analysis step. Only jobs which are listed in the `Flow` section are executed by CAP. Here is an example of the job called `vcf2bfile`:

```yaml
dataHandels:
    inVCF:
        disk:
            path: hdfs:///users/me/input.vcf.bgz
    outBFILE:
        disk:
            path: hdfs:///users/me/output.bfile
jobs:
    vcf2bfile:
        spec:
            operation: bypass
        dataHandles:
            inData:
                dataHandle: inVCF
                microOperations:
                    -   type: maf
                        parameter:
                            min: 0.05
                    -   type: count
            outData:
                dataHandle: outBFILE

        isProduced: True
        BlaBla: Bla
```

This example job is linked to the most basic operation in CAP called `bypass`.
As its name suggest, it does nothing but to bypass the only input (`inData`) to the only output (`outData`).
Yet there are alot you can do with bypass.
In this example the input and output are connected to `inVCF` and `outBfile` dataHandles respectively.
As a consequence the VCF file is convereted to plink bfile format (bim, bed, fam).
`microOperation` are samll operations applied to the input and output of the jobs.
In this example we apply two micro opperation to the input:
- Filter variants where minor allele frequency (maf) is greater than 5%
- Count the number of variants and samples in the dataset. 
When *microOperations* applied to an input their effect is only available during the execution of the job.
They do not affect the input source.
Output micro 


The input(s) and output(s) of operations are pointers to DataHandles. Each operation in the workflow file is linked to a pre-coded operation implmented in CAP. 


