# CAP is under construction. It will be ready soon.
# Cohort Analysis Platform (CAP)

Think of the concept of `Infrastructure as Code` used in the cloud environment:

You describe the infrastructure `--->>>` The cloud provids the infrastructure

CAP uses the same concept to provide genomic cohort analysis:

You describe the analysis `--->>>` CAP executes the analysis

The isolation between the description and the execution of the analysis simplifies maintenance of the analysis pipeline. Furthermore, it opens the path to Cohort Analysis as a Service (CAS) to appear in the future. CAP also facilitate report generation process.

CAP reads the specification of the analysis (the workflow) and executes all analysis steps (jobs). From this perspective, CAP is a simple workflow manager. Currently, CAP executes jobs serially and in a given order without checking for dependencies. There are many advanced workflow managers such as NextFlow and SnakeMake offering more features. **Our contribution is beyond introducing another workflow manager and our focus is on:**
- Designing the specification that best suits describing genomic cohort analysis. 
- Offering a set of pre-coded and configurable operations that allow seamless communication between various genomic software such as *Hail*, *VEP* and many more.
- Executing the analysis in one process (where possible) so the cohort and other data flow in the memory throughout the analysis. Note that writing and reading large genomic data to/from the disk is time-consuming. This is especially unacceptable for a chain of small operations performed on a large cohort where the intermediate data is not of interest.
- Parallelising the workload over multiple compute nodes (where possible).

## What is Genomic Cohort Analysis
A genomic cohort is a massive table of thousands of samples (i.e. humans) and tens of millons of features (genomic variants). There are clinical attributes for each sample such as ethnicity and gender, and genomic attributes for each variant such as if the variant located in a gene or affect a protein structure.

There are endless questions to answer.
- Can we cluster samples by their genome?
- What are underlying distributions?
- Can we compute the risk of a particular disease based on genetic data?
- What part of the genome is responsible for a disease?

Answering one question a the time, we are unlocking the secrets of the genome.

`We are leaving in an era where life decoding itself.`

## What is CAP

CAP is a simple workflow manager specialized for genomic cohort analysis (see [ReadMe](../README.md)).
Here is a brief explanatio of the terminology used.

Glossary:
- **Operation & Job**: Think of a function and a function-call. We have the same concept here. The Operation is like a function and the Job is like a function-call. Operations are implemented in the CAP source code. Jobs are defined in the workflow file. Functions have arguments and a return value. Operations have DataHandles (input and output data) and Parameters. Currently, limited number of operations are implmented in CAP. More operations will be introduced with future version.
- **Workflow**: A file that describes the analysis.
- **DataHandle**: DataHandle is the specification of data. Where and how the data is stored permanently? How to load data into memory? 
- **MicroOperation**: Imagine you want to make some tiny changes to a table just before passing the table to a function (without modifying the original table). Or, you want to make some changes to the output of the function. MicroOperations are designed for this cases. They act on a signle DataHandle just before being used or after being produced. 
- **Runtime** is the information about execution of the workflow (i.e. if a job is compeleted). CAP constantly updates runtime information in the workflow as the analysis is progressed. 
- **ExecutionPlan** is a list of job names to be executed in the same orde they appear in the list<sup id="ret_exec_plan">[1](#fn_exec_plan)</sup>.
- **SchemaCheck**: Errors in the workflow could cuase runtime exceptions. As a consecuence, CAP havily check the workflow at different stages of the execution to make sure of its correctness. There checks are mainly done by SchemaCheck (JSON Schema) but some of the checks are performed by python code.

Notes: 
- CAP can be executed in command line and through a python program (see [Geting Started](GetingStarted.md)).
- CAP requiers a path to the workflow file to be executed.
- If a CAP process is re-executed after a failour (assuming you fix the cause of the failour), it skips all the jobs which are completed and continue from where it fails. This is possible using the runtime information that are stored in workflow file permanently.
- CAP makes changes to the workflow. This includes the inferred and runtime information. The resulting workflow my not look neat. It is recomended to have a back up of the workflow prior to the execution.
## Learn More aboutCAP

If you don't know `YAML`, watch this 4-minute tutorial [here](https://youtu.be/0fbnyS_lHW4).
This is helpful when you read the following documents.

You may first try our step-by-step tutorials in [Getting Started](docs/GetingStarted.md) to have a better feeling of how CAP work.

The following documents explain different aspect of the CAP in more details:
- [Workflow](docs/Workflow.md): Specification of what can be in a workflow file.
- [DataHandle](docs/DataHandle.md): Specification of a DataHandle in the workflow as well as the flow of data in the program.
- [Operations](docs/Operations.md): List of all Operations and their specification.
- [MicroOperations](docs/MicroOperations.md): List of all MicorOperations and their specification.

Next you may read either [Describe Your Analysis](docs/DescribeAnalysis.md) or [Code Architecture](docs/CodeArch.md). The former hepls user to learn how to understand the provided pipelines be able to modify them or start a new analysis from scratch. The latter, is useful if you are a python programmer and would like expand the functionality of the CAP.

Finally [Explore Data](docs/ExploreData.md) describes set of utility to help you to explore the result of analysis performed by CAP.

## Installation
You can simply install cap using pip.
```bash
pip install cap-genomics
```

Note that [Hail](https://hail.is) is a requierment of the CAP. Hail has a few dependencies which must be installed prior to Hail installation. See [here](https://hail.is/docs/0.2/getting_started.html#installing-hail) for more details.





