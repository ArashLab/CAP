# Cohort Analysis Platform (CAP)

## Before Running the Examples
It is recomended to copy the 'examples' directory into another directory as workload files gets updated and you loose their initial state

```bash
git clone https://github.com/ArashLab/CAP.git
cd CAP
cp -r examples temp
cd temp
```

Note that hail is a requierment of cap and pyspark is a reqierment of hail. Thus, when you install cap using pip (as described above) pyspark is also installed on your system. If you haven't had previous spark configuration, the newly installed pyspark execute hail in local mode. Otherwise, you may experience difficulties following these examples.
## Example 1 (Import Genotype from a VCF)‍

The following workload described in yaml only include one stage where the function is **ImportGenotype**. This function requires input and output genotype file (inGT and outGt). Here is the content of workload file. 

```yaml
order:
    - IGTVCF
stages:
    IGTVCF:
        inout:
            inGt:
                direction: input
                path: 1kg.micro.vcf.bgz
            outGt:
                direction: output
                path: 1kg.ma.mt
        spec:
            function: ImportGenotype
```

To execute this workload run the main module in the cap package with the example workload file.

```bash
python -m cap.main --workload Example_01.yaml
```

## Example 2 (Write Result to TSV)

## Example 3 (Quality Metircs)

## Example 4 (Write to MySQL)

## Example 5 (VEP Annotations)

