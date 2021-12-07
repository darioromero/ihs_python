# ihs_python
## Objective

Site to share python code to read comma separated production history. These are export files originated on the IHS database.

## Input Files

## Output Files - Structure

The program xxxxx.py creates three (3) comma separated files:
* Header file (**298fHeaderOutput.csv**) with general information and geographical coordinates of the wells.
    + Attributes:
        - "id", "uid", "uid_source"
* Monthly Production history file (**298fProductionOutput.csv**).
    + Attributes:
        - "id", "pdate", "liquid_bbl", "gas_mcf", "water_bbl"
* Production Tests file (**298fTestOutput.csv**).
    + Attributes:
        - "id", "testNumber", "testDate", "upperPerfDepth", "lowerPerfDepth"

Those files are created in such a way that their format can be easily uploaded to any relational database.

## Programming in Python

The purpose of this exercise is twofold:

1. Learn Python by doing some coding
2. Use the files produced by the python programs in some analysis using R

Final purpose of all of this practice is to be able to identify some trends, common patterns, correlations 
and posibly some causation of what could be the performance signature of a group of wells.

As of today I have been taking some courses for the certification in Data Mining and Data Science and this 
data could potentially be of use for a proposed project I could outline from the analysis of the data.

Suggestions, comments and constructive feedback will be welcome !!!

## Update Jan-2019
Added a Python file that parse a datafile with more than 107 million records to capture 638264 wells with production history in the Permian Basin in Texas, USA.


