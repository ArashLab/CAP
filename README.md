# CAP
Cohort Analysis Platform
---
## What is CAP?
In short, CAP is going to automate some of the common genomic processing pipelines and facilitate analysis of the outcomes by creating astonishing reports. As a simple example, think of a genomic case-control study also known as Genome-Wide Association Study (GWAS). Here is the simplest workflow you can imagine:

1. Get the data into the format supported by the software you are using.
2. Compute quality metrics for samples and variants (SNPs), and then prune (clean up) data.
3. Perform Principal Component Analysis (PCA), create plots, and make sure the case and control group are not separated in the plot.
4. Perform Logistic-Regression or other statistical tests to identify the association power of variants.
5. Create a manhattan plot and dive into the region with a strong association to find a convincing argument.

Unfortunately, the simple pipeline above may not reveal the answer in one go. You may need to iterate through this many times, changing the parameters, and adding more and more steps like relatedness check and etc. If you are unlucky and couldn't find the answer, you need to go for a different pipeline like rare-variant, polygenic, epistatic or complex-disease analysis each of which requires many iterations to be tuned for your data.

You may run tens or hundreds of experiments with the datasets which makes it very difficult to keep track of everything and manage all the data you have produced.

Also, you have to deal with a chicken or the egg paradox too. If you don't perform an analysis with perfection (i.e. create the most efficient report and visualisation of the outcome), you may miss the important information your pipeline is capable of discovering. On the other hand, You may not have enough time for this perfection as many of the analysis simply fail and you need to move on quickly. But then how could you be sure if the analysis is useless if you don't do it with perfection.

That is why **we feel the necessity of automation for widely-used genomic workflows**. CAP is our response to this necessity. CAP is going to perform a wide range of analysis on your cohort and provide you with a comprehensive reports. By studying these reports you get an in-depth understanding of your data and plan your research more effectively by focusing on the analysis that seems promising. **CAP could not give you the ultimate answer but help you to move in the right direction towards the answer.**

## CAP, Hail and Spark (Also non-Spark)
[Hail](https://hail.is/) is a python library for the analysis of genomic data (and more) with an extensive list of functionalities. Hail is developed on top of [Apache Sapark](https://spark.apache.org/) that allow to process data on a cluster of computers (or a single computer if you wish). That means 100GB of data is no longer a big deal. **CAP uses Hail as the main analysis platform.** However, CAP is not limited to hail and could integrate other tools no matter if they are implemented in Spark (i.e. [Glow](https://glow.readthedocs.io/en/latest/blogs/glowgr-blog/glowgr-blog.html)) or they are ordinary software (i.e. [plink](https://www.cog-genomics.org/plink/), [bcftools](http://samtools.github.io/bcftools/bcftools.html) and [VEP](https://asia.ensembl.org/info/docs/tools/vep/script/index.html)).

## How CAP works?
All the processing steps and their parameters are described in a workload file (yaml or json). Reading genotype data from a VCF file, breaking multi-allelic sites and computing Hardy-Weinberg statistics are three example steps in a workload. Each processing step is linked to one of the functions implemented in CAP. You can change the parameters, reorder, add or delete steps. But if you want to add a novel step (i.e. impute sex with a new algorithm), you need to implement its logic in the CAP source code first.

**CAP reads the workload file and execute every single step.** Note that CAP is designed to automate some of the standard and routine pipelines and would not be as flexible as working with Hail or any other tools directly. Yet we try to provide as much flexibility as possible through parameters.