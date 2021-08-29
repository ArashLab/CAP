# MicroOperations

Imagine you want to make some tiny changes to the input data just before passing it to a function without modifying the original input data so that the change is only visible in the function.
Or, you want to make some changes to the output of the function.
MicroOperations are attached to input and output data ([DataHandle](DataHandle.md)) of a function ([job](Job.md)) to make this happen.
They act on a signle DataHandle just before being used or after being produced in a job.
A list of MicroOperations could be attached to each DataHanlde of a job.
MicroOprations are applied in the same order they appear in the list.
If DataHandle is input of the job, the effect of the MicroOperations is only visible within the execution of the job.
If DataHandle is ouput of the job, the effect of the MicroOperations remains permanently.

Currently MicroOperations act only on the memory interface of the DataHandle.
The same MicroOperation can be used to act on diferent data type (memory format).
At this stage only HailTable and HailMatrixTable are supported.

## rows cols lines fields
**Don't be confused about the rows and cols (columns).**
- There **is** a `rows` table in the HailMatrixTable.
- There **is** a `cols` table in the HailMatrixTable.
- There **are** one or more 'rows' in each table. We use the term `lines` to refer rows of a table.
- There **are** one or more 'cols' in each table. We use the term `fields` to refer cols of a table.

## List of MicroOperations
Here for each MicroOperation, we provide the
- Supported Types
- Parameters
- Notes

Three parameters are commonly used in MicroOperations. So we list them here to prevent replication of their definition.
- `axis`: The target table in the HailMatrixTable. Note that HailMatrixTable consist of four HailTables: `rows`, `cols`, `globals` and `entries`. Some MicroOperations requier to know on which table of the HailMatrixTable they should act. `axis` parameter must not present for other types.
- `fields`: List of existing fields in the table
- `newFields`: A dictionay with one or more items. Each item is a definition of a new field where *key* is the name for the new field and value is the expression to calculate the new field.

### General MicroOperations

- **`addIndex`**: Add a new field of integer that index lines of the table (starting from 0).
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: Can be `row` or `col` without 's' at the end
        - `name`: Name of the index field


- **`annotate`**: Add one or more field computed using expressions
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: Can be `rows`, `cols`, `globals` or `entries`
        - `newFields`: To be added

- **`drop`**: Drop one or more fields of the table at once
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `fields`: To drop

- **`keyBy`**: Key table by one or more fields
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: Can be `rows` or `cols`
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

- **`subsample`**: Subsample lines of the table
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: Can be `rows` or `cols`
        - `prob`: Probability ($0 \le prob \le 1$) of keeping the line in the output (Default is 0.1)
        - `seed`: Random seed (Optional with no default)
    - Notes:
        - `prob` can be considered as an approximate fraction of the lines kept in the output

- **`select`**: Select one or more fields of the table along with all key fields
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Parameters:
        - `axis`: Can be `rows`, `cols`, `globals` or `entries`
        - `fields`: To be selected
        - `newFields`: To be selected

- **`count`**: Print the number of lines in CAP-log and console
    - Supported Types:
        - HailTable
        - HailMatrixTable
    - Notes:
        - For HailMatrixTable this MicorOperation counts line in both `rows` and `cols` tables

- **`describe`**: Print information about data including schema in CAP-log and console
    - Supported Types:
        - HailTable
        - HailMatrixTable

### Specialised MicroOperations

- **`addId`**: Add `sampleId` field to cols table and `variantId` field to rows table of the HailMatrixTable. These fields can be used to link tables together.
    - Supported Types:
        - HailMatrixTable
    - Parameters:
        - `sampleId`: The field in the cols table to be assigned as sampleId
        - `variantId`: The only acceptable value is `CHR:POS:ALLELES` that results in a string formd by chromosome id, possition in choromosome and list of alleles respectively separated by a ':'
    - Notes:
        - The cols table is keyed by `sampleId` after performing this MicroOperation

- **`maf`**: Filter variants (rows) with a given range of Minor Allele Frequency (MAF)
    - Supported Types:
        - HailMatrixTable
    - Parameters:
        - `min`: Minimum maf ($0 \le prob \le 0.5$) (Default is 0)
        - `max`: Maximum maf ($0 \le prob \le 0.5$) (Default is 0.5)
    - Notes:
        - The range is inclusive

- **`ldPrune`**: Prune uncorrelated variants in a Linkage Disequilibrium (LD)
    - Supported Types:
        - HailMatrixTable
    - Parameters:
        - `r2`: Squared correlation threshold ($0 \le r2 \le 1$) (Default is 0.2)
        - `bp_window_size`: Maximum maf (Default is 1,000,000)
        - `memory_per_core`: Memory used per core in MB (Default is 256)
        - `keep_higher_maf`: (Default is True)
    - Notes:
        - See Hail documentation [here](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.ld_prune) for details

- **`splitMulti`**: Split multi-allelic variants to multiple bi-allelic variants
    - Supported Types:
        - HailMatrixTable
    - Parameters:
        - `withHTS`: Which Hail function to use (see Notes)
        - `keep_star`: keep * alleles (Default is False)
        - `left_aligned`: Normalise variant by aligning them to left (Default is False)
        - `vep_root`: Only used for HTS version (see Notes) (Default is 'vep')
        - `permit_shuffle`: (see Notes) (Default is False)
    - Notes:
        - Hail offers two functions to split multi-allelic sites: [split_multi](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.split_multi) and [split_multi_hts](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.split_multi_hts). We strongly recommend using the HTS version even if HTS info is not presented in your data. The other function may cause errors in subsequent analysis
        - For `vep_root` see [here](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.split_multi_hts)
        - For 1000 genome data you need to set this to `True`

- **`forVep`**: Prepare for exporting to VCF for VEP annotation (see notes)
    - Supported Types:
        - HailMatrixTable
    - Notes:
        - Drop all sampls (cols) and keep the following variant (rows) fields:
            - `locus`
            - `alleles`
            - `rsid`: set to `variantId` to appear in the VEP output annotation
        - The cols table is keyed by `sampleId` field that is set to a fix value of `dummy`. This is necessary for VCF export function






