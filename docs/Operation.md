# Operations

following operations are supported:

There are two types of operations: internal and extranal (see ???)
External operations work with the disk interface of a DataHandle as they don't share the memory with CAP<sup id="ret_shared_mem">[1](#fn_shared_mem)</sup>.
Internal operations usually access memory interface of the DataHandle.
When an internal operation produce (write) data in the memory interface, the **DataHandler** (a CAP module) immediately writes the data into the disk interface to be stored permanently (if DataHandle is not temporary).
The data persist in memory and subsequent operations can read it from the memory.
When an internal operation read data from the memory interface that is not loaded yet

**If external operation update data which is already loaded to memory how to update it? Note that there is no overwrite**

External op should have a python interface for submition and wait.
there is an operaion to create RamDisk on nodes

## Bypass

## PCA

## Join

## Summarise (CALC QC)

## Foot Notes
<a name="fn_shared_mem">1</a>: There are ways to share the memory between independent process. While we can share the CAP memory the external operator need to be able to read from the shared memory too. This may requier modification the the source code of the tool used in the external operation. Alternatives to this are using pipes or [RamDisk](https://en.wikipedia.org/wiki/RAM_drive). However, pipes need data serialisation and deserialisation. Also, RamDisk only speedup the disk interface of a DataHandle and does not allow to share the memory interface. [↩](#ret_shared_mem)

