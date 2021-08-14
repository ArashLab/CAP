# Cohort Analysis Platform (CAP)

Think of the concept of Infrastructure as Code (IaC) used in the cloud environment. The infrastructure is described. The cloud provids the infrastructure.

CAP uses the same concept to provide genomic cohort analyses. The analysis is described. CAP executes the analysis.

The isolation between the description and the execution of the analysis simplifies maintenance of the analysis pipeline and opens the path to implement serverless analysis in the future.

The [Tutorial](docs/Tutorial.md) page is a quick walk through to get started with CAP.
## What is Cohort Analysis
Genomic cohort consists of genetic variations of a number of samples compared to a reference sample. Common analyses include:
- Clustring (PCA) to predict ethnicity
- Association analysis to identify genetic variants relevant to a phenotype ()
- Sex imputation
- Rare variant analysis
- Polygenic risk score

We aim to expand CAP beyond its title in the future (i.e. RNA seq).

## How the Analysis is Described
The analysis is described in a YAML (or JSON) file refered as `Analysis File`. To know more read [CAP Architecture](docs/CAP_Architecture.md) and [Analysis Specification](docs/Analysis_Specification.md).

## How the Analysis is Executed
CAP uses a wide range of avaialble tools to perform the analysis. These tools are wrapped by CAP in forms of operations.

## Installation
You can simply install cap using pip.
```bash
pip install cap-genomics
```

Note that [Hail](https://hail.is) is a requierment of the CAP. Hail has a few dependencies which must be installed prior to Hail installation. See [here](https://hail.is/docs/0.2/getting_started.html#installing-hail) for more details.





