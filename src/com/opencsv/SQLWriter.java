package com.opencsv;

import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class SQLWriter extends CSVWriter {
    public static char COLUMN_ENCLOSER = '"';
    public static int maxLineWidth = 1500;
    protected String columns;
    private String fileHeader = "";
    private String[] columnTypes;

    public SQLWriter(Writer writer) {
        super(writer, ',', '\'', '\'', "\n");
        setBufferSize(INITIAL_BUFFER_SIZE);
    }

    public SQLWriter(String fileName) throws IOException {
        super(fileName, ',', '\'', '\'', "\n");
        setBufferSize(INITIAL_BUFFER_SIZE);
    }

    public void setMaxLineWidth(int width) {
        maxLineWidth = width;
    }

    public void writeNextRow(Object[] nextLine) throws IOException {
        if (nextLine == null) return;
        if (totalRows == 0) writeLog(0);
        add(columns);
        lineWidth = 2;
        int counter = 0;
        for (int i = 0; i < Math.min(nextLine.length, this.columnTypes.length); i++) {
            if (this.columnTypes[i] == null) continue;
            if (titles != null && excludes.containsKey(titles[i].toUpperCase()) && excludes.get(titles[i].toUpperCase()))
                continue;
            if (++counter > 1) add(separator);
            if (lineWidth > maxLineWidth) {
                add(lineEnd).add("    ");
                lineWidth = 4;
            }
            if (remaps.containsKey(titles[i])) {
                String nextElement = remaps.get(titles[i]);
                add(nextElement == null ? "null" : nextElement);
            } else {
                String nextElement = nextLine[i] == null ? null : nextLine[i].toString();
                Boolean isString = nextElement != null && !this.columnTypes[i].equals("double") && !this.columnTypes[i].equals("boolean");
                if (isString) {
                    add(quotechar);
                    if (nextElement.lastIndexOf(quotechar) >= 0) processLine(nextElement);
                    else add(nextElement);
                    add(quotechar);
                } else {
                    add(nextElement == null ? "null" : nextElement);
                }
            }
        }
        add(");").add(lineEnd);
        ++totalRows;
        flush(false);
    }

    private void init(String[] titles, String headerEncloser, int maxLineWidth) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(buffer.fileName).append("(");
        int counter = 0;
        this.titles = titles;
        for (int i = 0; i < titles.length; i++) {
            String title = titles[i].trim();
            this.titles[i] = title.toUpperCase();
            if (excludes.containsKey(this.titles[i]) && excludes.get(this.titles[i])) continue;
            if (++counter > 1) sb.append(",");
            if (!this.titles[i].matches("[A-Z][\\w\\$#]+$")) title = COLUMN_ENCLOSER + title + COLUMN_ENCLOSER;
            sb.append(headerEncloser).append(title).append(headerEncloser);
        }
        sb.append(")").append(lineEnd).append("  VALUES(");
        columns = sb.toString();
        lineWidth = 0;
        this.maxLineWidth = maxLineWidth;
        add(fileHeader == null ? "" : fileHeader).add(lineEnd);
    }

    public void setFileHead(String header) {
        fileHeader = header;
    }

    public void setCSVDataTypes(ResultSet rs) throws SQLException {
        resultService = new ResultSetHelperService(rs);
    }

    public int writeAll2SQL(ResultSet rs) throws Exception {
        return writeAll2SQL(rs, "", 9999);
    }

    public int writeAll2SQL(ResultSet rs, String headerEncloser, int maxLineWidth) throws Exception {
        try (SQLWriter s = this; ResultSetHelperService resultService = new ResultSetHelperService(rs)) {
            this.resultService = resultService;
            init(resultService.columnNames, headerEncloser, maxLineWidth);
            this.columnTypes = resultService.columnTypes;
            if (asyncMode) {
                resultService.startAsyncFetch(new RowCallback() {
                    @Override
                    public void execute(Object[] row) throws Exception {
                        writeNextRow(row);
                    }
                }, false);
            } else {
                Object[] values;
                while ((values = resultService.getColumnValues()) != null) writeNextRow(values);
            }
            return totalRows;
        }
    }

    public int writeAll2SQL(CSVReader reader) throws IOException {
        return writeAll2SQL(reader, "", 9999);
    }

    public int writeAll2SQL(String CSVFileSource) throws IOException {
        this.CSVFileName = CSVFileSource;
        return writeAll2SQL(new CSVReader(new FileReader(CSVFileSource)));
    }

    public int writeAll2SQL(String CSVFileSource, ResultSet rs) throws IOException, SQLException {
        this.CSVFileName = CSVFileSource;
        if (rs != null && !rs.isClosed()) setCSVDataTypes(rs);
        return writeAll2SQL(new CSVReader(new FileReader(CSVFileSource)));
    }

    public int writeAll2SQL(CSVReader reader, String headerEncloser, int maxLineWidth) throws IOException {
        try (SQLWriter w = this) {
            String[] array = reader.readNext();
            String types[] = new String[array.length];
            for (int i = 0; i < array.length; i++) types[i] = "string";
            if (resultService != null && resultService.columnNames != null) {
                ArrayList<String> cols = new ArrayList<String>();
                for (int j = 0; j < array.length; j++) {
                    boolean match = false;
                    for (int i = 0; i < resultService.columnNames.length; i++)
                        if (resultService.columnNames[i].equalsIgnoreCase(array[j].trim())) {
                            array[j] = resultService.columnNames[i];
                            types[j] = resultService.columnTypes[i];
                            cols.add(resultService.columnNames[i]);
                            match = true;
                        }
                    if (!match) types[j] = null;
                }
                createOracleCtlFileFromHeaders(CSVFileName, array, types, reader.getParser().getQuotechar(), reader.seprator, "\\r\\n");
                array = cols.toArray(new String[cols.size()]);
            }
            init(array, headerEncloser, maxLineWidth);
            this.columnTypes = types;
            while ((array = reader.readNext()) != null) {
                writeNextRow(array);
            }
            return totalRows;
        }
    }
}
