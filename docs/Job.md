# Job
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

