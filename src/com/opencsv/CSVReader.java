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

import com.opencsv.stream.reader.LineReader;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A very simple CSV reader released under a commercial-friendly license.
 * <p>
 * <b>Thread Safety:</b> This class is not thread-safe. If multiple threads
 * need to access a CSVReader instance, external synchronization is required.
 *
 * @author Glen Smith
 */
public class CSVReader implements Closeable, Iterable<String[]> {
    public static final boolean DEFAULT_KEEP_CR = false;
    public static final boolean DEFAULT_VERIFY_READER = true;
    /**
     * The default line to start reading.
     */
    public static final int DEFAULT_SKIP_LINES = 0;
    public static final int READ_AHEAD_LIMIT = Character.SIZE / Byte.SIZE;
    public char separator;
    private CSVParser parser;
    private int skipLines;
    private BufferedReader br;
    private LineReader lineReader;
    private boolean hasNext = true;
    private boolean linesSkipped;
    private boolean keepCR;
    private boolean verifyReader;
    private boolean closed;
    private long linesRead = 0;
    private long recordsRead = 0;

    /**
     * Constructs CSVReader using a comma for the separator.
     *
     * @param reader the reader to an underlying CSV source.
     */
    public CSVReader(Reader reader) {
        this(reader, CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVReader from a file path with automatic charset detection.
     *
     * @param filePath the path to the CSV file
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if filePath is null
     */
    public CSVReader(String filePath) throws IOException {
        this(createReaderWithCharset(filePath, null), CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVReader from a file path with automatic charset detection and custom separator.
     *
     * @param filePath  the path to the CSV file
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param escape    the character to use for escaping a separator or quote
     * @param encoding  charset to use for reading the file
     *                  (null/empty/"auto" for auto-detection)
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if filePath is null
     */
    public CSVReader(String filePath, char separator, char quotechar, char escape, String encoding) throws IOException {
        this(createReaderWithCharset(filePath, encoding), DEFAULT_SKIP_LINES, new CSVParser(separator, quotechar, escape));
    }

    /**
     * Constructs CSVReader from a file path with automatic charset detection and custom separator.
     *
     * @param filePath  the path to the CSV file
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param escape    the character to use for escaping a separator or quote
     * @param line      the line number to skip for start reading
     * @param encoding  charset to use for reading the file
     *                  (null/empty/"auto" for auto-detection)
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if filePath is null
     */
    public CSVReader(String filePath, char separator, char quotechar, char escape, int line, String encoding) throws IOException {
        this(createReaderWithCharset(filePath, encoding), line, new CSVParser(separator, quotechar, escape));
    }

    /**
     * Constructs CSVReader with supplied separator.
     *
     * @param reader    the reader to an underlying CSV source.
     * @param separator the delimiter to use for separating entries.
     */
    public CSVReader(Reader reader, char separator) {
        this(reader, DEFAULT_SKIP_LINES, new CSVParser(separator, CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER));
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     *
     * @param reader    the reader to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     */
    public CSVReader(Reader reader, char separator, char quotechar) {
        this(reader, separator, quotechar, CSVParser.DEFAULT_ESCAPE_CHARACTER, DEFAULT_SKIP_LINES, CSVParser.DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader with supplied separator, quote char and quote handling
     * behavior.
     *
     * @param reader       the reader to an underlying CSV source.
     * @param separator    the delimiter to use for separating entries
     * @param quotechar    the character to use for quoted elements
     * @param strictQuotes sets if characters outside the quotes are ignored
     */
    public CSVReader(Reader reader, char separator, char quotechar, boolean strictQuotes) {
        this(reader, separator, quotechar, CSVParser.DEFAULT_ESCAPE_CHARACTER, DEFAULT_SKIP_LINES, strictQuotes);
    }

    /**
     * Constructs CSVReader.
     *
     * @param reader    the reader to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param escape    the character to use for escaping a separator or quote
     */
    public CSVReader(Reader reader, char separator, char quotechar, char escape) {
        this(reader, separator, quotechar, escape, DEFAULT_SKIP_LINES, CSVParser.DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader.
     *
     * @param reader    the reader to an underlying CSV source.
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param line      the line number to skip for start reading
     */
    public CSVReader(Reader reader, char separator, char quotechar, int line) {
        this(reader, separator, quotechar, CSVParser.DEFAULT_ESCAPE_CHARACTER, line, CSVParser.DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader.
     *
     * @param reader       the reader to an underlying CSV source.
     * @param separator    the delimiter to use for separating entries
     * @param quotechar    the character to use for quoted elements
     * @param escape       the character to use for escaping a separator or quote
     * @param line         the line number to skip for start reading
     * @param strictQuotes sets if characters outside the quotes are ignored
     */
    public CSVReader(Reader reader, char separator, char quotechar, char escape, int line) {
        this(reader, separator, quotechar, escape, line, CSVParser.DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader.
     *
     * @param reader       the reader to an underlying CSV source.
     * @param separator    the delimiter to use for separating entries
     * @param quotechar    the character to use for quoted elements
     * @param escape       the character to use for escaping a separator or quote
     * @param line         the line number to skip for start reading
     * @param strictQuotes sets if characters outside the quotes are ignored
     */
    public CSVReader(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes) {
        this(reader, separator, quotechar, escape, line, strictQuotes, CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
    }

    /**
     * Constructs CSVReader with all data entered.
     *
     * @param reader                  the reader to an underlying CSV source.
     * @param separator               the delimiter to use for separating entries
     * @param quotechar               the character to use for quoted elements
     * @param escape                  the character to use for escaping a separator or quote
     * @param line                    the line number to skip for start reading
     * @param strictQuotes            sets if characters outside the quotes are ignored
     * @param ignoreLeadingWhiteSpace it true, parser should ignore white space before a quote in a field
     */
    public CSVReader(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes, boolean ignoreLeadingWhiteSpace) {
        this(reader, line, new CSVParser(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace));
    }

    /**
     * Constructs CSVReader with all data entered.
     *
     * @param reader                  the reader to an underlying CSV source.
     * @param separator               the delimiter to use for separating entries
     * @param quotechar               the character to use for quoted elements
     * @param escape                  the character to use for escaping a separator or quote
     * @param line                    the line number to skip for start reading
     * @param strictQuotes            sets if characters outside the quotes are ignored
     * @param ignoreLeadingWhiteSpace if true, parser should ignore white space before a quote in a field
     * @param keepCR                  if true the reader will keep carriage returns, otherwise it will discard them.
     */
    public CSVReader(Reader reader, char separator, char quotechar, char escape, int line, boolean strictQuotes, boolean ignoreLeadingWhiteSpace, boolean keepCR) {
        this(reader, line, new CSVParser(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace), keepCR, DEFAULT_VERIFY_READER);
    }

    /**
     * Constructs CSVReader with supplied CSVParser.
     *
     * @param reader    the reader to an underlying CSV source.
     * @param line      the line number to skip for start reading
     * @param csvParser the parser to use to parse input
     */
    public CSVReader(Reader reader, int line, CSVParser csvParser) {
        this(reader, line, csvParser, DEFAULT_KEEP_CR, DEFAULT_VERIFY_READER);
    }

    /**
     * Constructs CSVReader with supplied CSVParser.
     *
     * @param reader       the reader to an underlying CSV source.
     * @param line         the line number to skip for start reading
     * @param csvParser    the parser to use to parse input
     * @param keepCR       true to keep carriage returns in data read, false otherwise
     * @param verifyReader true to verify reader before each read, false otherwise
     * @throws IllegalArgumentException if reader or csvParser is null
     */
    CSVReader(Reader reader, int line, CSVParser csvParser, boolean keepCR, boolean verifyReader) {
        if (reader == null) {
            throw new IllegalArgumentException("Reader cannot be null");
        }
        if (csvParser == null) {
            throw new IllegalArgumentException("CSVParser cannot be null");
        }
        this.br = (reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader));
        this.lineReader = new LineReader(br, keepCR);
        this.skipLines = line;
        this.parser = csvParser;
        this.keepCR = keepCR;
        this.separator = csvParser.getSeparator();
        this.verifyReader = verifyReader;
    }

    /**
     * Creates a Reader with automatic charset detection from a file path.
     *
     * @param filePath the path to the CSV file
     * @param encoding the charset to use for reading the file
     *                 (null/empty/"auto" for auto-detection)
     * @return a Reader with detected or specified charset
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if filePath is null
     */
    private static Reader createReaderWithCharset(String filePath, String encoding) throws IOException {
        if (filePath == null) {
            throw new IllegalArgumentException("File path cannot be null");
        }
        String charset = encoding;
        if (charset == null || charset.trim().isEmpty() || "auto".equalsIgnoreCase(charset.trim())) {
            charset = CharsetDetector.detectCharset(filePath);
            System.out.println("    Detected file encoding: " + charset);
        } else {
            if (!java.nio.charset.Charset.isSupported(charset)) {
                throw new IllegalArgumentException("Unsupported charset: " + charset);
            }
        }
        return new java.io.InputStreamReader(new java.io.FileInputStream(filePath), charset);
    }

    /**
     * @return the CSVParser used by the reader.
     */
    public CSVParser getParser() {
        return parser;
    }

    /**
     * Returns the number of lines in the csv file to skip before processing.  This is
     * useful when there is miscellaneous data at the beginning of a file.
     *
     * @return the number of lines in the csv file to skip before processing.
     */
    public int getSkipLines() {
        return skipLines;
    }

    /**
     * Returns if the reader will keep carriage returns found in data or remove them.
     *
     * @return true if reader will keep carriage returns, false otherwise.
     */
    public boolean keepCarriageReturns() {
        return keepCR;
    }

    /**
     * Reads the entire file into a List with each element being a String[] of
     * tokens.
     * <p>
     * <b>Note:</b> This method will close the reader after reading all data.
     * If you need to keep the reader open for further operations, use readNext()
     * instead.
     *
     * @return a List of String[], with each String[] representing a line of the
     * file.
     * @throws IOException if bad things happen during the read
     */
    public List<String[]> readAll() throws IOException {
        List<String[]> allElements = new ArrayList<>();
        IOException exception = null;
        try {
            while (hasNext) {
                String[] nextLineAsTokens = readNext();
                if (nextLineAsTokens != null) {
                    allElements.add(nextLineAsTokens);
                }
            }
        } catch (IOException e) {
            exception = e;
            throw e;
        } finally {
            try {
                close();
            } catch (IOException e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    throw e;
                }
            }
        }
        return allElements;
    }

    /**
     * Reads the next line from the buffer and converts to a string array.
     *
     * @return a string array with each comma-separated element as a separate
     * entry.
     * @throws IOException if bad things happen during the read
     */
    public String[] readNext() throws IOException {
        String[] result = null;
        try {
            do {
                String nextLine = null;
                do {
                    nextLine = getNextLine();
                    if (!hasNext) {
                        return validateResult(result);
                    }
                } while (nextLine == null || nextLine.trim().isEmpty());
                String[] r = parser.parseLineMulti(nextLine);
                if (r.length > 0) {
                    if (result == null) {
                        result = r;
                    } else {
                        result = combineResultsFromMultipleReads(result, r);
                    }
                }
            } while (parser.isPending());
        } catch (IOException e) {
            hasNext = false;
            throw e;
        }
        return validateResult(result);
    }

    private String[] validateResult(String[] result) {
        if (result != null) {
            recordsRead++;
        }
        return result;
    }

    /**
     * For multi line records this method combines the current result with the result from previous read(s).
     *
     * @param buffer   - previous data read for this record
     * @param lastRead - latest data read for this record.
     * @return String array with union of the buffer and lastRead arrays.
     */
    private String[] combineResultsFromMultipleReads(String[] buffer, String[] lastRead) {
        String[] t = new String[buffer.length + lastRead.length];
        System.arraycopy(buffer, 0, t, 0, buffer.length);
        System.arraycopy(lastRead, 0, t, buffer.length, lastRead.length);
        return t;
    }

    /**
     * Reads the next line from the file.
     *
     * @return the next line from the file without trailing newline
     * @throws IOException if bad things happen during the read
     */
    private String getNextLine() throws IOException {
        if (isClosed()) {
            hasNext = false;
            return null;
        }

        if (!this.linesSkipped) {
            for (int i = 0; i < skipLines; i++) {
                lineReader.readLine();
                linesRead++;
            }
            this.linesSkipped = true;
        }
        String nextLine = lineReader.readLine();
        if (nextLine == null) {
            hasNext = false;
        } else {
            linesRead++;
        }

        return hasNext ? nextLine : null;
    }

    /**
     * Checks to see if the file is closed.
     *
     * @return true if the file can no longer be read from.
     */
    private boolean isClosed() {
        if (closed) {
            return true;
        }
        if (!verifyReader) {
            return false;
        }
        try {
            br.mark(READ_AHEAD_LIMIT);
            int nextByte = br.read();
            br.reset();
            if (nextByte == -1) {
                closed = true;
                return true;
            }
            return false;
        } catch (IOException e) {
            closed = true;
            return true;
        }
    }

    /**
     * Closes the underlying reader.
     *
     * @throws IOException if the close fails
     */
    public void close() throws IOException {
        if (!closed) {
            IOException exception = null;
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                exception = e;
            } finally {
                closed = true;
            }
            if (exception != null) {
                throw exception;
            }
        }
    }

    /**
     * Creates an Iterator for processing the csv data.
     *
     * @return an String[] iterator.
     * @throws RuntimeException if unable to read data from the reader
     */
    public Iterator<String[]> iterator() {
        try {
            return new CSVIterator(this);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create CSV iterator", e);
        }
    }

    /**
     * Returns if the CSVReader will verify the reader before each read.
     * <p/>
     * By default the value is true which is the functionality for version 3.0.
     * If set to false the reader is always assumed ready to read - this is the functionality
     * for version 2.4 and before.
     * <p/>
     * The reason this method was needed was that certain types of Readers would return
     * false for its ready() method until a read was done (namely readers created using Channels).
     * This caused opencsv not to read from those readers.
     *
     * @return true if CSVReader will verify the reader before reads.  False otherwise.
     * @link https://sourceforge.net/p/opencsv/bugs/108/
     * @since 3.3
     */
    public boolean verifyReader() {
        return this.verifyReader;
    }

    /**
     * Used for debugging purposes this method returns the number of lines that has been read from
     * the reader passed into the CSVReader.
     * <p/>
     * Given the following data.
     * <code>
     * <pre>
     * First line in the file
     * some other descriptive line
     * a,b,c
     *
     * a,"b\nb",c
     * </pre>
     * </code>
     * With a CSVReader constructed like so
     * <code>
     * <pre>
     * CSVReader c = builder.withCSVParser(new CSVParser())
     *                      .withSkipLines(2)
     *                      .build();
     * </pre>
     * </code>
     * The initial call to getLinesRead will be 0.<br>
     * After the first call to readNext() then getLinesRead will return 3 (because header was read).<br>
     * After the second call to read the blank line then getLinesRead will return 4 (still a read).<br>
     * After third call to readNext getLinesRead will return 6 because it took two line reads to retrieve this record.<br>
     * Subsequent calls to readNext (since we are out of data) will not increment the number of lines read.<br>
     * <p/>
     * An example of this is in the linesAndRecordsRead() test in CSVReaderTest.
     *
     * @return the number of lines read by the reader (including skip lines).
     * @link https://sourceforge.net/p/opencsv/feature-requests/73/
     * @since 3.6
     */
    public long getLinesRead() {
        return linesRead;
    }

    /**
     * Used for debugging purposes this method returns the number of records that has been read from
     * the CSVReader.
     * <p/>
     * Given the following data.
     * <code>
     * <pre>
     * First line in the file
     * some other descriptive line
     * a,b,c
     * <p/>
     * a,"b\nb",c
     * </pre></code>
     * With a CSVReader constructed like so
     * <code><pre>
     * CSVReader c = builder.withCSVParser(new CSVParser())
     *                      .withSkipLines(2)
     *                      .build();
     * </pre></code>
     * The initial call to getRecordsRead will be 0.<br>
     * After the first call to readNext() then getRecordsRead will return 1.<br>
     * After the second call to read the blank line then getRecordsRead will return 2
     * (a blank line is considered a record with one empty field).<br>
     * After third call to readNext getRecordsRead will return 3 because even though
     * reads to retrieve this record it is still a single record read.<br>
     * Subsequent calls to readNext (since we are out of data) will not increment the number of records read.<br>
     * <p/>
     * An example of this is in the linesAndRecordsRead() test in CSVReaderTest.
     *
     * @return the number of records (Array of Strings[]) read by the reader.
     * @link https://sourceforge.net/p/opencsv/feature-requests/73/
     * @since 3.6
     */
    public long getRecordsRead() {
        return recordsRead;
    }
}
