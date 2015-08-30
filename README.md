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
