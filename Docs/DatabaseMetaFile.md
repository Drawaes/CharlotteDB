The database metadata file is the first file written and first file read in a folder. A number of settings cannot be changed in this file and if they are, it could lead to corruption. 
There is a second settings file that defines various runtime settings that a user can tweak on the fly.

First there will be a magic number

0xF7F7_3232_2323_7F7F

The file consists of sections

# File overview

MAGICNUMBER
-------
Section 1
-------
Section 2
-------
Section 3

Each section consists of 

# Section

Section Length - Int32
-------------
Length Prefixed UTF8 String
-------------
Section Specific Content
-------------
64bit Hash with 1 byte "Type" indicator

