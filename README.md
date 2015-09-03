# OpenCSV
Based on OpenCSV on http://opencsv.sourceforge.net, please visit the address for its original usage.
Requires JRE 1.7+.

# Additional Features
    - Performance and memory optimization by NIO, buffering and the reduction of object creations 
    - Convert ResultSet to SQL file, refer to SQLWriter.writeAll2SQL(<Resuleset>,...) 
    - Convert CSV file to SQL file, refer to SQLWriter.writeAll2SQL(<CSVFilePath>,...)
    - Convert ResultSet/CSV to Oracle SQL*Loader Files(Automatically)
    - Support automatically compression if destination file extension is ".zip" or ".gz"
    - Support Multi-threads processing to speed up performance, refer to CSVWriter.setAsyncMode(boolean) 

# Performance
In Async mode, the program spends very few time on converting fields and writing file.<br/>
Almost all the time is used on fetching data, so the performance depends on network speed.<br/>
A as test result on Oracle database in local machine, generating 2.5 millions records into a file with 780 MB size, only taking external 2 seconds on writing, total time cost is 25 seconds . 