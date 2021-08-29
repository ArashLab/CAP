# CAP Architecture in Short

CAP is a simple workflow manager specialized for genomic cohort analysis (see [ReadMe](../README.md)).

Glossary:
- **DataHandle** is the specification of a data used in the analysis (see [DataHandle](DataHandle.md))
- **Operation** is a function that implments an analysis step (see [Operation](Operation.md)). Each operation act on a set of input and output DataHandles based on given parameters
- **Job** is the specification of a step in the analysis. Each job is linked to an operation and specifies DataHandles and parameters used to execute the operation. Currently, limited number of operations are implmented. More operations will be introduced with future version.
- **Workflow** refers to a file that contains detailed description of the analysis.


CAP reads the workflow and constantly updates its runtime as the analysis is progressed.
runtime is the information about execution of the analysis (i.e. if a job is compeleted).
To minimise errors in the execution, CAP performs several schemaCheck on the workflow at different stages to make sure the workflow contains valid information.
schemaChecks help you to indentify and fix erros in the workflow as early as possible.
If a CAP process is re-executed after a failour, it skips all the jobs which are completed and continue from where it fails.

CAP is a basic workflow manager. The executionPlan (written by the user) defines the order of jobs to be executed. Currently CAP only support a simple linear execution of the jobs and does not check the data dependency between the jobs.


## Workflow File
The workflow describes the analysis in *YAML* or *JSON* format. These formats are simple, human-readable and widely used to store configuration data. If you don't know *YAML*, don't worry.  Also, the [Geting Started](GetingStarted.md) includes a few examples that can help you understand these formats.

CAP requires specific information to be presented in the workflow. The details are provided in [Describe Your Analysis](docs/DescribeAnalysis.md).

The workflow file includes information about the following:
- 
- config is the configuration of the analysis (i.e. default values)
- 


### Jobs:
A Job is the specification of a step in the analysis. Only jobs which are listed in the executionPlan section are executed by CAP. Here is an example of the job called vcf2bfile:

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

This example job is linked to the most basic operation in CAP called bypass.
As its name suggest, it does nothing but to bypass the only input (inData) to the only output (outData).
Yet there are alot you can do with bypass.
In this example the input and output are connected to inVCF and outBfile dataHandles respectively.
As a consequence the VCF file is convereted to plink bfile format (bim, bed, fam).
microOperation are samll operations applied to the input and output of the jobs.
In this example we apply two micro opperation to the input:
- Filter variants where minor allele frequency (maf) is greater than 5%
- Count the number of variants and samples in the dataset. 
When *microOperations* applied to an input their effect is only available during the execution of the job.
They do not affect the input source.
Output micro 


# Comment
There are two types of operations: internal and extranal (see ???)
External operations work with the disk interface of a DataHandle as they don't share the memory with CAP<sup id="ret_shared_mem">[1](#fn_shared_mem)</sup>.
Internal operations usually access memory interface of the DataHandle.
When an internal operation produce (write) data in the memory interface, the **DataHandler** (a CAP module) immediately writes the data into the disk interface to be stored permanently (if DataHandle is not temporary).
The data persist in memory and subsequent operations can read it from the memory.
When an internal operation read data from the memory interface that is not loaded yet

**If external operation update data which is already loaded to memory how to update it? Note that there is no overwrite**

External op should have a python interface for submition and wait.
there is an operaion to create RamDisk on nodes