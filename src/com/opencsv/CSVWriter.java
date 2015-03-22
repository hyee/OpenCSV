package com.opencsv;

/**
 Copyright 2005 Bytecode Pty Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A very simple CSV writer released under a commercial-friendly license.
 *
 * @author Glen Smith
 */
public class CSVWriter implements Closeable, Flushable {

    public static final int INITIAL_STRING_SIZE = 128;
    /**
     * The character used for escaping quotes.
     */
    public static final char DEFAULT_ESCAPE_CHARACTER = '"';
    /**
     * The default separator to use if none is supplied to the constructor.
     */
    public static final char DEFAULT_SEPARATOR = ',';
    /**
     * The default quote character to use if none is supplied to the
     * constructor.
     */
    public static final char DEFAULT_QUOTE_CHARACTER = '"';
    /**
     * The quote constant to use when you wish to suppress all quoting.
     */
    public static final char NO_QUOTE_CHARACTER = '\u0000';
    /**
     * The escape constant to use when you wish to suppress all escaping.
     */
    public static final char NO_ESCAPE_CHARACTER = '\u0000';
    /**
     * Default line terminator uses platform encoding.
     */
    public static final String DEFAULT_LINE_END = "\n";
    protected Writer rawWriter;
    protected PrintWriter pw;
    protected char separator;
    protected char quotechar;
    protected char escapechar;
    protected int buffeWidth;
    protected int lineWidth;
    protected String lineEnd;
    protected String tableName;
    protected String CSVFileName;
    protected StringBuilder sb;
    protected DeflaterOutputStream zipStream;
    protected String zipType = null;
    protected String extensionName = "csv";
    protected ResultSetHelper resultService = new ResultSetHelperService();
    protected String[] columnTypes;
    protected String[] columnNames;
    protected int INITIAL_BUFFER_SIZE = 16384;


    /**
     * Constructs CSVWriter using a comma for the separator.
     *
     * @param writer the writer to an underlying CSV source.
     */
    public CSVWriter(Writer writer) {
        this(writer, DEFAULT_SEPARATOR);
    }

    public CSVWriter(String fileName, char separator, char quotechar, char escapechar, String lineEnd) throws IOException {
        //this(new FileWriter(fileName));
        this(new FileWriter(fileName), separator, quotechar, escapechar, lineEnd);
        this.CSVFileName = fileName;
        tableName = new File(fileName).getName();
        int index = tableName.lastIndexOf(".");
        if (index > -1) {
            String extName = tableName.substring(index + 1);
            tableName = tableName.substring(0, index);
            if (extName.equalsIgnoreCase("zip") || extName.equalsIgnoreCase("gz")) {
                pw.close();
                rawWriter.close();
                FileOutputStream out = new FileOutputStream(fileName);
                zipType = extName.toLowerCase();
                if (zipType.equals("zip")) {
                    ZipOutputStream zip = new ZipOutputStream(out);
                    zip.putNextEntry(new ZipEntry(tableName + "." + extensionName));
                    zipStream = zip;
                } else zipStream = new GZIPOutputStream(out, true);
                rawWriter = new OutputStreamWriter(out);
                pw = new PrintWriter(rawWriter);
            }
        }
    }

    public CSVWriter(String fileName) throws IOException {
        this(fileName, DEFAULT_SEPARATOR, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END);
    }


    /**
     * Constructs CSVWriter with supplied separator.
     *
     * @param writer    the writer to an underlying CSV source.
     * @param separator the delimiter to use for separating entries.
     */
    public CSVWriter(Writer writer, char separator) {
        this(writer, separator, DEFAULT_QUOTE_CHARACTER);
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
        this.rawWriter = writer;
        this.pw = new PrintWriter(writer);
        this.separator = separator;
        this.quotechar = quotechar;
        this.escapechar = escapechar;
        this.lineEnd = lineEnd;
        sb = new StringBuilder(INITIAL_BUFFER_SIZE);
    }

    public void setBufferSize(int bytes) {
        INITIAL_BUFFER_SIZE = bytes;
        sb = new StringBuilder(INITIAL_BUFFER_SIZE);
    }

    protected CSVWriter add(char str) {
        sb.append(str);
        ++buffeWidth;
        ++lineWidth;
        return this;
    }

    protected CSVWriter add(String str) {
        int len = str.length();
        sb.append(str);
        buffeWidth += len;
        lineWidth += len;
        return this;
    }

    protected CSVWriter add(StringBuilder sbf) {
        return add(sbf.toString());
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
        int i = 1;
        for (String[] line : allLines) {
            writeNext(line, applyQuotesToAll);
            ++i;
        }
        return i;
    }

    /**
     * Writes the entire list to a CSV file. The list is assumed to be a
     * String[]
     *
     * @param allLines a List of String[], with each String[] representing a line of
     *                 the file.
     */
    public int writeAll(List<String[]> allLines) throws IOException {
        int i = 1;
        for (String[] line : allLines) {
            writeNext(line);
            ++i;
        }
        return i;
    }

    /**
     * Writes the column names.
     *
     * @param rs - ResultSet containing column names.
     * @throws SQLException - thrown by ResultSet::getColumnNames
     */
    protected void writeColumnNames(ResultSet rs) throws SQLException, IOException {
        columnNames = resultService.getColumnNames(rs);
        columnTypes = resultService.getColumnTypes(rs);
        writeNext(columnNames);
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
    public int writeAll(java.sql.ResultSet rs, boolean includeColumnNames) throws SQLException, IOException {
        return writeAll(rs, includeColumnNames, true);
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
    public int writeAll(java.sql.ResultSet rs, boolean includeColumnNames, boolean trim) throws SQLException, IOException {
        if (includeColumnNames) {
            writeColumnNames(rs);
            if (CSVFileName != null)
                createOracleCtlFileFromHeaders(CSVFileName,columnNames, quotechar);
        }

        int i = 0;
        while (rs.next()) {
            ++i;
            writeNext(resultService.getColumnValues(rs, trim));
        }
        flush();
        return i;
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine         a string array with each comma-separated element as a separate
     *                         entry.
     * @param applyQuotesToAll true if all values are to be quoted.  false applies quotes only
     *                         to values which contain the separator, escape, quote or new line characters.
     */
    public void writeNext(String[] nextLine, boolean applyQuotesToAll) throws IOException {
        if (nextLine == null) {
            return;
        }

        lineWidth = 0;

        for (int i = 0; i < nextLine.length; i++) {

            if (i != 0) {
                add(separator);
            }

            String nextElement = nextLine[i];

            Boolean stringContainsSpecialCharacters = stringContainsSpecialCharacters(nextElement);

            if ((applyQuotesToAll || stringContainsSpecialCharacters) && quotechar != NO_QUOTE_CHARACTER) {
                add(quotechar);
            }

            if (stringContainsSpecialCharacters) {
                add(processLine(nextElement));
            } else {
                add(nextElement);
            }

            if ((applyQuotesToAll || stringContainsSpecialCharacters) && quotechar != NO_QUOTE_CHARACTER) {
                add(quotechar);
            }
        }

        add(lineEnd);
        if (buffeWidth >= INITIAL_BUFFER_SIZE) flush();
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine a string array with each comma-separated element as a separate
     *                 entry.
     */
    public void writeNext(String[] nextLine) throws IOException {
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
    protected StringBuilder processLine(String nextElement) {
        StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int j = 0; j < nextElement.length(); j++) {
            char nextChar = nextElement.charAt(j);
            processCharacter(sb, nextChar);
        }

        return sb;
    }

    /**
     * Appends the character to the StringBuilder adding the escape character if needed.
     *
     * @param sb       - StringBuffer holding the processed character.
     * @param nextChar - character to process
     */
    private void processCharacter(StringBuilder sb, char nextChar) {
        if (escapechar != NO_ESCAPE_CHARACTER && (nextChar == quotechar || nextChar == escapechar)) {
            sb.append(escapechar).append(nextChar);
        } else {
            sb.append(nextChar);
        }
    }

    /**
     * Flush underlying stream to writer.
     *
     * @throws IOException if bad things happen
     */
    public void flush() throws IOException {
        if (buffeWidth == 0) return;
        if (zipStream != null) {
            zipStream.write(sb.toString().getBytes());
            zipStream.flush();
        } else {
            pw.write(sb.toString());
            pw.flush();
        }
        buffeWidth = 0;
        sb.setLength(0);
    }

    /**
     * Close the underlying stream writer flushing any buffered content.
     *
     * @throws IOException if bad things happen
     */
    public void close() throws IOException {
        flush();
        if (zipStream != null) {
            if (zipType.equals("zip")) ((ZipOutputStream) zipStream).finish();
            if (!zipType.equals("zip")) ((GZIPOutputStream) zipStream).finish();
            zipStream.close();
            zipStream = null;
        }
        pw.flush();
        pw.close();
        rawWriter.close();
    }

    public void createOracleCtlFileFromHeaders(String CSVFileName, String[] titles, char encloser) throws IOException {
        String FileName = new File(CSVFileName).getParentFile().getAbsolutePath();
        FileName = FileName + File.separator + tableName + ".ctl";
        FileWriter writer = new FileWriter(FileName);
        StringBuilder b = new StringBuilder(INITIAL_STRING_SIZE);
        b.append("OPTIONS (SKIP=1)\nLOAD DATA\n");
        b.append("INFILE      ").append(tableName).append(".csv").append("\n");
        b.append("BADFILE     ").append(tableName).append(".bad").append("\n");
        b.append("DISCARDFILE ").append(tableName).append(".dsc").append("\n");
        b.append("APPEND INTO TABLE ").append(tableName).append("\n");
        b.append("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '").append(encloser).append("' TRAILING NULLCOLS\n(\n");
        for (int i = 0; i < titles.length; i++) {
            if (i > 0) b.append(",\n");
            b.append("    ").append(String.format("%-32s",titles[i]));
            if (columnTypes[i].equalsIgnoreCase("timestamp"))
                b.append("\"TO_DATE(:").append(titles[i]).append(",'YYYY-MM-DD HH24:MI:SS')\"");
            if (columnTypes[i].equalsIgnoreCase("timestamptz"))
                b.append("\"TO_TIMESTAMP(:").append(titles[i]).append(",'YYYY-MM-DD HH24:MI:SSXFF TZH')\"");
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
    public boolean checkError() {
        return pw.checkError();
    }

    /**
     * Sets the result service.
     *
     * @param resultService - the ResultSetHelper
     */
    public void setResultService(ResultSetHelper resultService) {
        this.resultService = resultService;
    }

    /**
     * flushes the writer without throwing any exceptions.
     */
    public void flushQuietly() {
        try {
            flush();
        } catch (IOException e) {
            // catch exception and ignore.
        }
    }
}
