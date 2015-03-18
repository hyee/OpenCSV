package com.opencsv;

import com.opencsv.util.StringUtils;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Will on 2015/3/18.
 */
public class SQLWriter extends CSVWriter {

    protected String columns;
    protected String[] columnTyes;
    protected int initSize;
    private int INITIAL_BUFFER_SIZE = 4000000;
    private int maxLineWidth;
    private int buffeWidth;
    private int lineWidth;
    private StringBuilder sb;

    public SQLWriter(Writer writer) {
        super(writer, ',', '\'', '\'', "\n");
    }

    public void writeAll(ResultSet rs) throws SQLException, IOException {
        writeAll(rs, "", 9999);
    }

    public void writeAll(ResultSet rs, String headerEncloser, int maxLineWidth) throws SQLException, IOException {
        columns = headerEncloser + StringUtils.join(resultService.getColumnNames(rs), headerEncloser + "," + headerEncloser) + headerEncloser;
        columnTyes = resultService.getColumnTypes(rs);
        sb = new StringBuilder(initSize);
        buffeWidth = 0;
        lineWidth = 0;
        this.maxLineWidth = maxLineWidth;
        while (rs.next()) {
            writeNextRow(resultService.getColumnValues(rs, false));
        }
        flushSB(true);

    }

    protected void flushSB(Boolean completed) throws IOException {
        if (buffeWidth == 0) return;
        pw.write(sb.toString());
        if (completed) flush();
        sb.delete(0, sb.length());
        buffeWidth = 0;
    }

    private SQLWriter add(String str) {
        int len = str.length();
        sb.append(str);
        buffeWidth += len;
        lineWidth += len;
        return this;
    }

    private SQLWriter add(char str) {
        sb.append(str);
        ++buffeWidth;
        ++lineWidth;
        return this;
    }

    public void writeNextRow(String[] nextLine) throws IOException {
        if (nextLine == null) {
            return;
        }
        add("INSERT INT TABLE_NAME(").add(columns).add(")").add(lineEnd).add(("VALUES("));
        lineWidth = 0;
        for (int i = 0; i < nextLine.length; i++) {
            if (i != 0) add(separator);
            if (lineWidth > maxLineWidth) {
                add(lineEnd);
                lineWidth = 0;
            }

            String nextElement = nextLine[i];
            int quotePos = -1;
            if (columnTyes[i] != "number" && columnTyes[i] != "boolean") {
                add(quotechar);
                quotePos = nextElement.lastIndexOf(quotechar);
            }

            if (quotePos != -1) {
                int orgSize = buffeWidth;
                add(nextElement);
                while (quotePos != -1) {
                    sb.insert(orgSize + quotePos, quotechar);
                    ++buffeWidth;
                    quotePos = nextElement.lastIndexOf(quotechar, quotePos - 1);
                }
            } else {
                add(nextElement.equals("") ? "null" : nextElement);
            }

            if (columnTyes[i] != "number" && columnTyes[i] != "boolean") add(quotechar);
        }
        add(");").add(lineEnd);
        if (buffeWidth >= INITIAL_BUFFER_SIZE - maxLineWidth) flushSB(false);
    }
}
