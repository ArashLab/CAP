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
To understand and use CAP, we recommend reading [CAP Architecture in Short](docs/CapArch.md) first. It helps you quickly identify if CAP is something you need or not by summarising the mental idea behind CAP and the way it is implemented. It also includes a short description of how the analysis should be described. Next you may try our step-by-step tutorials in [Getting Started](docs/GetingStarted.md) to have a better feeling of how CAP work.

Next you may read either [Describe Your Analysis](docs/DescribeAnalysis.md) or [Code Architecture](docs/CodeArch.md). The former hepls user to learn how to understand the provided pipelines be able to modify them or start a new analysis from scratch. The latter, is useful if you are a python programmer and would like expand the functionality of the CAP.

Finally [Explore Data](docs/ExploreData.md) describes set of utility to help you to explore the result of analysis performed by CAP.

## Installation
You can simply install cap using pip.
```bash
pip install cap-genomics
```

Note that [Hail](https://hail.is) is a requierment of the CAP. Hail has a few dependencies which must be installed prior to Hail installation. See [here](https://hail.is/docs/0.2/getting_started.html#installing-hail) for more details.





