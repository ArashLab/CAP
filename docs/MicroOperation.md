# MicroOperations

A MicroOperation is an action that act on signle DataHandle used in a job.
It may transform the DataHandle before being 
A list of MicroOperations could be attached to each DataHanlde linked to a job.
Micro oprations are applied in the same order they appear in the list.
If DataHandle is input of the job, the effect of the MicroOperations is only visible during the execution of the job.
If DataHandle is ouput of the hob, MicroOperations are applied just before the output is written and their effect remains in the output permanently.
Following micro operations are supported:

- **addIndex**: Add an integer index column (starting from 0) to a table.
    - Supported Types
        - HailTable
        - HailMatrixTable: `axis` parameter must be presented
    - Parameters:
        - `axis`: `row` or `col` (only for HailMatrixTable)
        - `name`: name of the index column

- **aggregate**: ???TBF(must be the same type as input)
    - Supported Types
        - HailTable
        - HailMatrixTable: `axis` parameter must be presented
    - Parameters:
        - `axis`: `row` or `col` (only for HailMatrixTable)
        - `exp`: Aggregation expression
        - `retType`: Type of data returnd by the agregator.

- **annotate**:
    - Supported Types
        - HailTable
        - HailMatrixTable: `axis` parameter must be presented
    - Parameters:
        - `axis`: `row` or `col` (only for HailMatrixTable)
        - `namedExpr` dict of key-value pair (annotate multiple field at the same time)
            - key is the field name
            - value is the annotation expression

- **count**: Print the count in log and console
    - Supported Types
        - HailTable
        - HailMatrixTable

- **drop**:
    - Supported Types
        - HailTable
        - HailMatrixTable
    - Parameters:
        - list of fields to be droped

- **keyBy**:
    - Supported Types
        - HailTable
        - HailMatrixTable: `axis` parameter must be presented
    - Parameters:
        - `axis`: `row` or `col` (only for HailMatrixTable)
        - `keys`: list of fields to be used as key
        - `namedKeys`: dict of key-value pair:
            - key is the key name
            - value is the key expression

- 




