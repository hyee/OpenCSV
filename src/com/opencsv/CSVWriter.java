package com.opencsv;

/**
 * Copyright 2005 Bytecode Pty Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * A very simple CSV writer released under a commercial-friendly license.
 *
 * @author Glen Smith
 */
public class CSVWriter implements Closeable {
    public static final String RFC4180_LINE_END = "\r\n";
    /**
     * The character used for escaping quotes.
     */
    public static char DEFAULT_ESCAPE_CHARACTER = '"';
    /**
     * The quote constant to use when you wish to suppress all quoting.
     */
    public static char NO_QUOTE_CHARACTER = '\u0000';
    /**
     * The escape constant to use when you wish to suppress all escaping.
     */
    public static char NO_ESCAPE_CHARACTER = '\u0000';
    /**
     * Default line terminator uses platform encoding.
     */
    public static String DEFAULT_LINE_END = "\n";
    public static int INITIAL_BUFFER_SIZE = 8 << 20; //8 MB
    protected char separator;
    protected char quotechar;
    protected char escapechar;
    protected int lineWidth;
    protected int totalRows;
    protected int incrRows;
    protected String lineEnd;
    protected PrintWriter logWriter;
    protected String CSVFileName;
    protected ResultSetHelperService resultService;
    protected FileBuffer buffer;
    protected boolean asyncMode = false;
    protected HashMap<String, Boolean> excludes = new HashMap<>();
    protected HashMap<String, String> remaps = new HashMap();
    protected String[] titles;

    /**
     * Constructs CSVWriter using a comma for the separator.
     *
     * @param writer the writer to an underlying CSV source.
     */
    public CSVWriter(Writer writer) {
        this(writer, CSVParser.DEFAULT_SEPARATOR);
    }

    public CSVWriter(String fileName, char separator, char quotechar, char escapechar, String lineEnd) throws IOException {
        this(new FileWriter(fileName), separator, quotechar, escapechar, lineEnd);
        this.CSVFileName = fileName;
        String extensionName = "csv";
        if (quotechar == '\'' && escapechar == quotechar) extensionName = "sql";
        buffer = new FileBuffer(INITIAL_BUFFER_SIZE, fileName, extensionName);
        logWriter = new PrintWriter(buffer.file.getParentFile().getAbsolutePath() + File.separator + buffer.fileName + ".log");
        //logWriter = new PrintWriter(System.err);
    }

    public CSVWriter(String fileName) throws IOException {
        this(fileName, CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END);
    }

    /**
     * Constructs CSVWriter with supplied separator.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries.
     */
    public CSVWriter(Writer writer, char separator) {
        this(writer, separator, CSVParser.DEFAULT_QUOTE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     */
    public CSVWriter(Writer writer, char separator, char quotechar) {
        this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer     the writer to an underlying CSV source.
     * @param separator  the delimiter to use for separating entries
     * @param quotechar  the character to use for quoted elements
     * @param escapechar the character to use for escaping quotechars or escapechars
     */
    public CSVWriter(Writer writer, char separator, char quotechar, char escapechar) {
        this(writer, separator, quotechar, escapechar, DEFAULT_LINE_END);
    }

    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param lineEnd   the line feed terminator to use
     */
    public CSVWriter(Writer writer, char separator, char quotechar, String lineEnd) {
        this(writer, separator, quotechar, DEFAULT_ESCAPE_CHARACTER, lineEnd);
    }

    /**
     * Constructs CSVWriter with supplied separator, quote char, escape char and line ending.
     *
     * @param writer     the writer to an underlying CSV source.
     * @param separator  the delimiter to use for separating entries
     * @param quotechar  the character to use for quoted elements
     * @param escapechar the character to use for escaping quotechars or escapechars
     * @param lineEnd    the line feed terminator to use
     */
    public CSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd) {
        this.separator = separator;
        this.quotechar = quotechar;
        this.escapechar = escapechar;
        this.lineEnd = lineEnd;
    }

    public void setExclude(String columnName, boolean exclude) {
        if (columnName == null) return;
        excludes.put(columnName.toUpperCase().trim(), exclude);
    }

    public void setRemap(String columnName, String value) {
        if (columnName == null) return;
        remaps.put(columnName.toUpperCase().trim(), value.trim());
    }

    public void setAsyncMode(boolean mode) {
        asyncMode = mode;
    }

    public void setBufferSize(int bytes) {
        INITIAL_BUFFER_SIZE = bytes;
    }

    protected CSVWriter add(char str) throws IOException {
        buffer.write(str);
        ++lineWidth;
        return this;
    }

    protected CSVWriter add(String str) throws IOException {
        buffer.write(str);
        int len = str.length();
        lineWidth += len;
        return this;
    }

    protected CSVWriter add(StringBuilder sbf) throws IOException {
        return add(sbf.toString());
    }

    protected void writeLog(int rows) {
        String msg = String.format("%s: %d rows extracted, total: %d rows, %.2f MB, %.3f secs on fetching.", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), rows - incrRows, rows, (float) buffer.position / 1024 / 1024, resultService == null ? 0f : (float) resultService.cost / 1e9);
        logWriter.write(msg + "\n");
        logWriter.flush();
        System.out.println("    " + msg);
        System.out.flush();
        incrRows = rows;
    }

    /**
     * Writes the entire list to a CSV file. The list is assumed to be a
     * String[]
     *
     * @param allLines         a List of String[], with each String[] representing a line of
     *                         the file.
     * @param applyQuotesToAll true if all values are to be quoted.  false if quotes only
     *                         to be applied to values which contain the separator, escape,
     *                         quote or new line characters.
     */
    public int writeAll(List<String[]> allLines, boolean applyQuotesToAll) throws IOException {
        try (CSVWriter c = this) {
            for (String[] line : allLines) {
                writeNext(line, applyQuotesToAll);
            }
            return totalRows;
        }
    }

    /**
     * Writes the entire list to a CSV file. The list is assumed to be a
     * String[]
     *
     * @param allLines a List of String[], with each String[] representing a line of
     *                 the file.
     */
    public int writeAll(List<String[]> allLines) throws IOException {
        return writeAll(allLines, false);
    }

    /**
     * Writes the column names.
     *
     * @throws SQLException - thrown by ResultSet::getColumnNames
     */
    protected void writeColumnNames() throws SQLException, IOException {
        writeNext(resultService.columnNames);
    }

    /**
     * Writes the entire ResultSet to a CSV file.
     * <p/>
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs                 the result set to write
     * @param includeColumnNames true if you want column names in the output, false otherwise
     * @throws java.io.IOException   thrown by getColumnValue
     * @throws java.sql.SQLException thrown by getColumnValue
     */
    public int writeAll(java.sql.ResultSet rs, boolean includeColumnNames) throws Exception {
        return writeAll(rs, includeColumnNames, false);
    }

    /**
     * Writes the entire ResultSet to a CSV file.
     * <p/>
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs                 the Result set to write.
     * @param includeColumnNames include the column names in the output.
     * @param trim               remove spaces from the data before writing.
     * @throws java.io.IOException   thrown by getColumnValue
     * @throws java.sql.SQLException thrown by getColumnValue
     */
    public int writeAll(java.sql.ResultSet rs, boolean includeColumnNames, boolean trim) throws Exception {
        try (CSVWriter c = this; ResultSetHelperService resultService = new ResultSetHelperService(rs)) {
            this.resultService = resultService;
            titles = new String[resultService.columnNames.length];
            for (int i = 0; i < titles.length; i++) titles[i] = resultService.columnNames[i].trim().toUpperCase();
            if (includeColumnNames) {
                writeColumnNames();
                if (CSVFileName != null)
                    createOracleCtlFileFromHeaders(CSVFileName, resultService.columnNames, resultService.columnTypes, quotechar, separator, null);
            }

            if (asyncMode) {
                resultService.startAsyncFetch(new RowCallback() {
                    @Override
                    public void execute(Object[] row) throws Exception {
                        writeNext(row);
                    }
                });
            } else {
                Object[] values;
                while ((values = resultService.getColumnValues(true)) != null) writeNext(values);
            }
            return totalRows;
        }
    }

    public void flush(boolean force) throws IOException {
        if (buffer.currentBytes == 0) return;
        if (buffer.flush(force)) writeLog(totalRows);
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine         a string array with each comma-separated element as a separate
     *                         entry.
     * @param applyQuotesToAll true if all values are to be quoted.  false applies quotes only
     *                         to values which contain the separator, escape, quote or new line characters.
     */
    public void writeNext(Object[] nextLine, boolean applyQuotesToAll) throws IOException {
        if (nextLine == null) {
            return;
        }
        if (totalRows == 0) writeLog(0);
        lineWidth = 0;
        int counter = 0;
        String nextElement;
        Boolean stringContainsSpecialCharacters;
        for (int i = 0; i < nextLine.length; i++) {
            if (titles != null && remaps.containsKey(titles[i])) nextElement = remaps.get(titles[i]);
            else nextElement = nextLine[i] == null ? "" : nextLine[i].toString();
            if (resultService != null && excludes.containsKey(resultService.columnNames[i].toUpperCase()) && excludes.get(resultService.columnNames[i].toUpperCase()))
                continue;
            if (++counter > 1) add(separator);
            stringContainsSpecialCharacters = stringContainsSpecialCharacters(nextElement);
            if ((applyQuotesToAll || stringContainsSpecialCharacters) && quotechar != NO_QUOTE_CHARACTER) {
                add(quotechar);
            }

            if (stringContainsSpecialCharacters) {
                processLine(nextElement);
            } else {
                add(nextElement);
            }

            if ((applyQuotesToAll || stringContainsSpecialCharacters) && quotechar != NO_QUOTE_CHARACTER) {
                add(quotechar);
            }
        }
        add(lineEnd);
        ++totalRows;
        flush(false);
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine a string array with each comma-separated element as a separate
     *                 entry.
     */
    public void writeNext(Object[] nextLine) throws IOException {
        writeNext(nextLine, false);
    }

    /**
     * checks to see if the line contains special characters.
     *
     * @param line - element of data to check for special characters.
     * @return true if the line contains the quote, escape, separator, newline or return.
     */
    protected boolean stringContainsSpecialCharacters(String line) {
        return line.indexOf(quotechar) != -1 || line.indexOf(escapechar) != -1 || line.indexOf(separator) != -1 || line.contains(DEFAULT_LINE_END) || line.contains("\r");
    }

    /**
     * Processes all the characters in a line.
     *
     * @param nextElement - element to process.
     * @return a StringBuilder with the elements data.
     */
    protected void processLine(String nextElement) throws IOException {
        for (int j = 0; j < nextElement.length(); j++) {
            char nextChar = nextElement.charAt(j);
            if (escapechar != NO_ESCAPE_CHARACTER && (nextChar == quotechar || nextChar == escapechar)) {
                add(escapechar).add(nextChar);
            } else {
                add(nextChar);
            }
        }
    }

    /**
     * Flush underlying stream to writer.
     *
     * @throws IOException if bad things happen
     */
    /**
     * Close the underlying stream writer flushing any buffered content.
     *
     * @throws IOException if bad things happen
     */
    public void close() throws IOException {
        flush(true);
        logWriter.close();
        buffer.close();
        resultService = null;
        System.gc();
        System.runFinalization();
    }

    private String toHexIfInvisible(char c) {
        String str = Integer.toHexString(c);
        return c >= 32 ? "'" + c + "'" : "X'" + (str.length() == 1 ? "0" + str : str) + "'";
    }

    public void createOracleCtlFileFromHeaders(String CSVFileName, String[] titles, String[] types, char encloser, char seperator, String rowSep) throws IOException {
        File file = new File(CSVFileName);
        String FileName = file.getParentFile().getAbsolutePath() + File.separator + buffer.fileName + ".ctl";
        String ColName, str;
        FileWriter writer = new FileWriter(FileName);
        StringBuilder b = new StringBuilder(CSVParser.READ_BUFFER_SIZE);
        b.append("OPTIONS (SKIP=1, ROWS=3000, BINDSIZE=16777216, STREAMSIZE=33554432, ERRORS=1000, READSIZE=16777216, DIRECT=FALSE)\nLOAD DATA\n");
        b.append("INFILE      ").append(buffer.fileName).append(".csv");
        if (rowSep != null) b.append(" \"STR '" + rowSep + "'\"");
        b.append("\n");
        b.append("BADFILE     ").append(buffer.fileName).append(".bad\n");
        b.append("DISCARDFILE ").append(buffer.fileName).append(".dsc\n");
        b.append("APPEND INTO TABLE ").append(buffer.fileName).append("\n");
        b.append("FIELDS CSV TERMINATED BY ").append(toHexIfInvisible(separator));
        b.append(" OPTIONALLY ENCLOSED BY ").append(toHexIfInvisible(encloser)).append(" AND ").append(toHexIfInvisible(encloser)).append(" TRAILING NULLCOLS\n(\n");
        for (int i = 0; i < titles.length; i++) {
            if (excludes.containsKey(titles[i].toUpperCase()) && excludes.get(titles[i].toUpperCase())) continue;
            if (i > 0) b.append(",\n");
            ColName = '"' + titles[i] + '"';
            b.append("    ").append(String.format("%-32s", ColName));
            if (types[i] == null) b.append("FILLER");
            else {
                if (types[i].equalsIgnoreCase("date")) b.append("DATE \"YYYY-MM-DD HH24:MI:SS\" ");
                else if (types[i].equalsIgnoreCase("timestamp")) b.append("TIMESTAMP \"YYYY-MM-DD HH24:MI:SSXFF\" ");
                else if (types[i].equalsIgnoreCase("timestamptz"))
                    b.append("TIMESTAMP WITH TIME ZONE \"YYYY-MM-DD HH24:MI:SSXFF TZH\" ");
                b.append(String.format("NULLIF %s=BLANKS", ColName));
            }
        }
        b.append("\n)");
        writer.write(b.toString());
        writer.flush();
        writer.close();
    }

    /**
     * Checks to see if the there has been an error in the printstream.
     *
     * @return <code>true</code> if the print stream has encountered an error,
     * either on the underlying output stream or during a format
     * conversion.
     */
    /**
     * Sets the result service.
     *
     * @param resultService - the ResultSetHelper
     */
    public void setResultService(ResultSetHelperService resultService) {
        this.resultService = resultService;
    }

    /**
     * flushes the writer without throwing any exceptions.
     */
}
