The University of Tokyo DPC project (codename Charsiu)
=======
 
The charis is Apache Pig UDF library for data transformation from log data into a wide table format data. 
This UDF project is started for clinical epidemiologic study using Japanese Diagnosis Procedure Combination Database data.
 
Quick Start
---------  

  Requiement Hadoop >=1.0.0, pig >=0.9.2

  Download https://raw.github.com/wiki/hiromasah/charsiu/releases/charsiu-udf-1.1.jar 

  Write "register /path/to/charsiu-udf-1.0.jar;" into Pig script.

AUTHOR
-------
product leader 
  Hiromasa Horiguchi (The University of Tokyo)
contributer
  Tatsuya Nakamura (Kurusugawa Computer, Inc.)
  Taisuke Sato (Kurusugawa Computer, Inc.)
  Toru Nishikawa (Preferred Infrastructure)

LICENCE
-------
Apache License Version 2.0

Update history
---------
Release charsiu-udf-1.1/charsiu-dpc-2011.1 2013/03/29
~~~~~~~~~
* added UDF MulticastEvaluate, LoadDataWithSchema
* modified a UDF StoreDataWithSchema for free encoding
* modified a specification of choosing file system for DPC data

Release charsiu-dpc-2011.0 2012/12/13
~~~~~~~~~
* added DPC schema of 2011

Release charsiu-udf-1.0/charsiu-dpc-2010.0 2012/10/31
~~~~~~~~~
* several bug fixes
* added license text
* setting for maven report

Release 0.1.0 2012/7/1
~~~~~~~~~
initial release
