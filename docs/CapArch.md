# CAP Architecture in Short

**CAP can be seen as a simple workflow manager.**
The *Workflow* is a text file that contains the description of the analysis.
CAP has a module that read the workflow file and constantly update it as the analysis is preogressed. To minimise runtime error, CAP performs several checks to make sure the workflow contains valid information before execution of each step. If a CAP process fails at one step and re-executed, it skips all the steps which are compeleted and continue from where it fails.

However, CAP main contributions are beyound being a simple workflow manager. There contributions are:
- A library of pre coded operations used in the genomic cohort analysis. These operation allow seamless communication between various genomic software such as Hail, VEP and many more.
- Execute multiple steps in one process (Data flow between steps in the memory where possible). Workflow managers are able to orchestrate independent steps executed in independent processes.
- Parallelise the workload over multiple compute nodes.

## Workflow File
Workflow file describes the analysis in *YAML* or *JSON* file format. These formats are simple, human-readable and widely used for configuration data. If you don't know *YAML*, don't worry. You can learn all you need to know in a 4 minutes tutoral [here](https://youtu.be/0fbnyS_lHW4). Also, the [Getting Started](GettingStarted.md) include a few example that can help you understand these formats.
CAP requiers specific information to be presented in the workflow file. The details are provided in [Describe Your Analysis](docs/DescribeAnalysis.md).

The workflow file include infromation about the following:
### DataFiles:
Specification of the data used or produced in the analysis. This include the file-system, path to the file, data format, compression and etc. Some data file are temporary and only live in the memory during the execution of the process. Temporary data file are not written into the disk. It is possible to skip definition of temporary data file here as they are generated on the fly.

### Stages:
specification of each analysis step. The input(s) and output(s) of each step is pointer to one of the file 


