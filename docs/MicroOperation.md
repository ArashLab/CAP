# MicroOperations

Imagine you want to make some tiny changes to a table just before passing the table to a function (without modifying the original table).
Or, you want to make some changes to the output of the function.
MicroOperations are designed for these cases.
They act on a signle [DataHandle](DataHandle.md) just before being used or after being produced in a job.
A list of MicroOperations could be attached to each DataHanlde of a [job](Job.md).
Micro oprations are applied in the same order they appear in the list.
If DataHandle is input of the job, the effect of the MicroOperations is only visible during the execution of the job.
If DataHandle is ouput of the hob, the effect of the MicroOperations remains in the output permanently.

Currently MicroOperations act only on the memory interface of a DataHandler.
The same MicroOperation can be used to act on diferent data type (memory format).
At this stage only HailTable and HailMatrixTable are supported.

## rows cols lines fields
**Don't be confused about the rows and cols (columns).**
- There **is** a `rows` table in the HailMatrixTable.
- There **is** a `cols` table in the HailMatrixTable.
- There **are** one or more 'rows' in each table. We use the term `lines` to refer rows of a table.
- There **are** one or more 'cols' in each table. We use the term `fields` to refer cols of a table.


## List of MicroOperations

Here for each MicroOperation, we provide the supported formats, parameters and notes.

Three parameters are commonly used in MicroOperations. So we list them here to prevent replication of their definition.
- `axis`: The target table in the MatrixTable. Note that HailMatrixTable consist of four HailTables: `rows`, `cols`, `globals` and `entries`. Some MicroOperations requier to know on which table of the HailMatrixTable they should act. `axis` parameter must not present for other types.
- `fields`: List of existing fields in the table
- `newFields`: A dictionay with one or more items. each item is definition of a new field where *key* is the name for the new field and value is the expression to calculate the new field.


- **`addIndex`**: Add an integer index (starting from 0) to the table.
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: `row` or `col` without 's' at the end
        - `name`: name of the field with integer index


- **`annotate`**: Add one or more field computed using expressions
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: `rows`, `cols`, `globals` or `entries`
        - `newFields`: To be annotated.

- **`count`**: Print the number of lines in CAP-log and console
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Notes:
        - For HailMatrixTable this MicorOperation counts line in both `rows` and `cols` tables

- **`drop`**: Drop one or more fields of the table at once
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `fields`: To drop.

- **`keyBy`**: Key table by one or more fields
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: `rows` or `cols` (only for HailMatrixTable)
        - `fields`: To be in the key list
        - `newFields`: To be in the key list

- **`rename`**: Rename one or more fields of the table at once
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `renames`: A dictionary with one or more items where
            - *Key* is the existing field name
            - *Value* is the new field name

- **`subsample`**: Subsample lines in the table
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: `rows` or `cols` (only for HailMatrixTable)
        - `prob`: Probability to keep the line. Must be between 0 and 1. (Default is 0.1).
        - `seed`: Random seed (Optional with no default)
    - Notes:
        - `prob` can be considered as approximate fraction of the line kept in the output.

- **`select`**: Select one or more fields of the table along with all key fields.
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: `rows`, `cols`, `globals` or `entries`
        - `fields`: To be selected
        - `newFields`: To be selected


