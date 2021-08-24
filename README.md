# CAP is under construction. It will be ready soon.
# Cohort Analysis Platform (CAP)

Think of the concept of `Infrastructure as Code` used in the cloud environment:

You describe the infrastructure `--->>>` The cloud provids the infrastructure

CAP uses the same concept to provide genomic cohort analysis:

You describe the analysis `--->>>` CAP executes the analysis

The isolation between the description and the execution of the analysis simplifies maintenance of the analysis pipeline. Furthermore, it opens the path to Cohort Analysis as a Service (CAS) in the future.
## What is Genomic Cohort Analysis
Simpley, a genomic cohort is a massive table of thousands of samples (i.e. humans) and tens of millons of features (genomic variants). There are clinical attributes for each sample such as ethnicity and gender and genomic attributes for each variant such as if the variant located in a gene or affect a protein structure.

And here is where the analysis begins. There are endless questions to ask.
- Can we cluster samples by their genome?
- What are underlying distributions?
- Can we compute the risk of a particular disease based on genetic data?
- What part of the genome is responsible for a disease?

Answering one question a the time we are unlocking the secrets of the genome.

`We are leaving in the eara where life decode the life.`

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





