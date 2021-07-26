
# Cohort Analysis Platform (CAP)


We have not prepared documentation yet. Here we briefly describe the structure of workload file. The examples described blow helps to better understand how things works.

The workload file (see this [example](examples/Example_01.yaml)) describe a dictionary with 4 keys:
* *config*: used to define all constant and avoid replications (i.e. file path which are output of one stage and input of another stage)
* *globConfig*: used to set global configurations such as default values and limits
* *order*: an array of stage names that defines the order in which stages must be executed
* *stages*: used to describe each stage with 3 keys
    * *spec*: Specification of the stage (i.e. which function to execute)
    * *io*: Inputs and Outputs of the stage.
    * *arg*: Arguments (parameters) passed to the stage.