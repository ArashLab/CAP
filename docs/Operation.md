# Operations

All operations work with memory interface of their DataHandles unless specified otherwise.

- **`bypass`**: Bypass is the most basic operation in CAP. As its name suggests, it does nothing but to bypass the only input (inData) to the only output (outData). Different formats are converted if possibe (see notes). Note that the same memory format can be linked to different disk formats (see the first Example).
    - `dataHandles`
        - `inData`
            - Supported Types
                - HailTable
                - HailMatrixTable
        - `outData`
            - Supported Types
                - HailTable
                - HailMatrixTable
    - `parameters`:
        - `axis`: Can be `rows`, `cols` or `globals` (see notes)
    - Notes:
        - When converting HailMatrixTable to HailTable, the `axis` parameter determind which part of the HailMatrixTable is bypassed.
        - When converting HailTable to HailMatrixTable, HailTable populates the rows table of the HailMatrixTable. The cols table remains empty.
    - Examples:
        - `inData` and `outData` disk format are VCF and PlinkBfile respectively while the memory format is a HailMatrixTable for both. In this case, a VCF file is converted to a PlinkBfile.
        - `inData` and `outData` disk format are VCF and MySQL-Table respectively. The memory format is a HailMatrixTable and HailTable respectively. `axis` is set to `rows`. In this case, all the variant information from VCF file are exported into a MySQL-Table.

- **`join`**: Joining two tables is so easy. The `join` operation here can perform a chain of joins. Not just joins but also filtering of a table based on another table (semi-join and anti-joins) as well as annotating one table using another. The chain can consist of series of HailTables and HailMatrixTables. The `order` parameter defines the order of data in the chain of joins and how to join them.
    - `dataHandles`
        - `inData_*`: The '*' can be replaces with anything to produce infinit number of input DataHandles used in the joining chain. Using `keyBy` MicroOperation the key of each table can be set to what is needed in the join. 
            - Supported Types
                - HailTable
                - HailMatrixTable
        - `outData`: 
            - Supported Types
                - HailTable
                - HailMatrixTable
    - `parameters`:
        - `order`: It is a list of dictionaries. Each dict point to a table and describe how it should be joined with others. The following field exist All dict has the `table` keys with the value of `inData_*` (point to one of the input data). Other keys determinse how this table must be join with others. The first dict in the list does not have any other keys as it is used as the base table. 
    - Notes:
        - When converting HailMatrixTable to HailTable, the `axis` parameter determind which part of the HailMatrixTable is bypassed.
        - When converting HailTable to HailMatrixTable, HailTable populates the rows table of the HailMatrixTable. The cols table remains empty.
    - Examples:
        - `inData` and `outData` disk format are VCF and PlinkBfile respectively while the memory format is a HailMatrixTable for both. In this case, a VCF file is converted to a PlinkBfile.
        - `inData` and `outData` disk format are VCF and MySQL-Table respectively. The memory format is a HailMatrixTable and HailTable respectively. `axis` is set to `rows`. In this case, all the variant information from VCF file are exported into a MySQL-Table.




## PCA

## Join

## Summarise (CALC QC)


=====================

There are two types of operations: internal and extranal (see ???)
External operations work with the disk interface of a DataHandle as they don't share the memory with CAP<sup id="ret_shared_mem">[1](#fn_shared_mem)</sup>.
Internal operations usually access memory interface of the DataHandle.
When an internal operation produce (write) data in the memory interface, the **DataHandler** (a CAP module) immediately writes the data into the disk interface to be stored permanently (if DataHandle is not temporary).
The data persist in memory and subsequent operations can read it from the memory.
When an internal operation read data from the memory interface that is not loaded yet

**If external operation update data which is already loaded to memory how to update it? Note that there is no overwrite**

External op should have a python interface for submition and wait.
there is an operaion to create RamDisk on nodes
## Foot Notes
<a name="fn_shared_mem">1</a>: There are ways to share the memory between independent process. While we can share the CAP memory the external operator need to be able to read from the shared memory too. This may requier modification the the source code of the tool used in the external operation. Alternatives to this are using pipes or [RamDisk](https://en.wikipedia.org/wiki/RAM_drive). However, pipes need data serialisation and deserialisation. Also, RamDisk only speedup the disk interface of a DataHandle and does not allow to share the memory interface. [↩](#ret_shared_mem)

