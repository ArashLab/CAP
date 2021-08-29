# CAP Architecture in Short

CAP is a simple workflow manager specialized for genomic cohort analysis (see [ReadMe](../README.md)).
Here is a brief explanatio of the terminology used.

Glossary:
- **Operation** & **Job**: Think of a function and a function-call. We have the same concept here. The Operation is like a function and the Job is like a function-call. Operations are implemented in the CAP source code. Jobs are defined in the workflow file. Functions have arguments and a return value. Operations have DataHandles (input and output data) and Parameters. Currently, limited number of operations are implmented in CAP. More operations will be introduced with future version.
- **Workflow** is a file that describes the analysis. It includes: 
    - **Jobs** as defined above.
    - **DataHandles** which are specification of a data (i.e. path to a file, its format and compression). 
    - **Runtime** is the information about execution of the workflow (i.e. if a job is compeleted). CAP constantly updates runtime information in the workflow as the analysis is progressed. 
    - **ExecutionPlan** is a list of job names to be executed in the same orde they appear in the list<sup id="ret_exec_plan">[1](#fn_exec_plan)</sup>.
- **SchemaCheck**: Errors in the workflow could cuase runtime exceptions. As a consecuence, CAP havily check the workflow at different stages of the execution to make sure of its correctness. There checks are mainly done by SchemaCheck (JSON Schema) but some of the checks are performed by python code.

Notes: 
- CAP can be executed in command line and through a python program (see [Geting Started](GetingStarted.md)).
- CAP requiers a path to the workflow file to be executed.
- If a CAP process is re-executed after a failour (assuming you fix the cause of the failour), it skips all the jobs which are completed and continue from where it fails. This is possible using the runtime information that are stored in workflow file permanently.
- CAP makes changes to the workflow. This includes the inferred and runtime information. The resulting workflow my not look neat. It is recomended to have a back up of the workflow prior to the execution.
## Workflow
The workflow describes the analysis in *YAML* or *JSON* format.
If you don't know `YAML`, watch this 4-minute tutorial [here](https://youtu.be/0fbnyS_lHW4).
These formats are simple, human-readable and widely used to store configuration data.
The following information can present in the workflow. 
- DataHandles
- Jobs
- ExecutionPlan
- config is the configuration of the analysis (i.e. default values)
- 

See [Workflow](Workflow.md) document for compelete description


### Jobs:
A Job is the specification of a step in the analysis. Only jobs which are listed in the ExecutionPlan section are executed by CAP. Here is an example of the job called vcf2bfile:

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


## Foot Notes
exec_plan

<a name="fn_exec_plan">1</a>: Since each job is parallelised oever several compute nodes to harvest entire compute resources, job-level parallelisation is not critical. Yet, we plan to offer job-level parallelisation in the future. Ultimately, CAP will identify the dependencies between jobs automatically and identify the execution plan dynamically (to be implemented). [↩](#ret_exec_plan)


<a name="fn_shared_mem">1</a>: There are ways to share the memory between independent process. While we can share the CAP memory the external operator need to be able to read from the shared memory too. This may requier modification the the source code of the tool used in the external operation. Alternatives to this are using pipes or [RamDisk](https://en.wikipedia.org/wiki/RAM_drive). However, pipes need data serialisation and deserialisation. Also, RamDisk only speedup the disk interface of a DataHandle and does not allow to share the memory interface. [↩](#ret_shared_mem)

