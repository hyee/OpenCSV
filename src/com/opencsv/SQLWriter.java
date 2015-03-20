package com.opencsv;

import com.opencsv.util.StringUtils;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SQLWriter extends CSVWriter {

    protected String columns;
    protected String[] columnTyes;
    protected String[] columnNames;
    protected int initSize;
    private int maxLineWidth;
    private String tableName = "<table>";
    private String fileHeader="";

    public SQLWriter(Writer writer) {
        super(writer, ',', '\'', '\'', "\n");
        setBufferSize(4000000);
    }

    public SQLWriter(String fileName) throws IOException {
        this(new FileWriter(fileName));
        tableName = new File(fileName).getName();
        int index=tableName.lastIndexOf(".");
        if(index>-1) tableName = tableName.substring(0, index);
    }

    public void setCSVDataTypes(ResultSet rs) throws SQLException{
        columnTyes=resultService.getColumnTypes(rs);
        columnNames=resultService.getColumnNames(rs);
    }

    public void setMaxLineWidth(int width) {maxLineWidth=width;}

    public void writeNextRow(String[] nextLine) throws IOException {
        if (nextLine == null) return;
        add(columns);
        lineWidth = 2;
        for (int i = 0; i < nextLine.length; i++) {
            if (i != 0) add(separator);
            if (lineWidth > maxLineWidth) {
                add(lineEnd).add("    ");
                lineWidth = 4;
            }
            String nextElement = nextLine[i];
            int quotePos = -1;
            Boolean isQuote=columnTyes[i] != "number" && columnTyes[i] != "boolean" && !nextElement.equals("");

            if (isQuote) {
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
            if (isQuote) add(quotechar);
        }
        add(");").add(lineEnd);
        if (buffeWidth >= INITIAL_BUFFER_SIZE - maxLineWidth) flush();
    }

    private void init(String[] titles, String headerEncloser, int maxLineWidth) {
        columns = headerEncloser + StringUtils.join(titles, headerEncloser + "," + headerEncloser) + headerEncloser;
        columns = "INSERT INTO " + tableName + "(" + columns + ")" + lineEnd + "  VALUES(";
        sb = new StringBuilder(initSize);
        buffeWidth = 0;
        lineWidth = 0;
        this.maxLineWidth = maxLineWidth;
        add(fileHeader).add(lineEnd);
    }

    public void setFileHead(String header) {
        fileHeader=header;
    }

    public void writeAll2SQL(ResultSet rs) throws SQLException, IOException {
        writeAll2SQL(rs, "", 9999);
    }

    public void writeAll2SQL(ResultSet rs, String headerEncloser, int maxLineWidth) throws SQLException, IOException {
        init(resultService.getColumnNames(rs), headerEncloser, maxLineWidth);
        columnTyes = resultService.getColumnTypes(rs);
        while (rs.next()) writeNextRow(resultService.getColumnValues(rs, false));
        close();
    }

    public void writeAll2SQL(CSVReader reader) throws IOException {
        writeAll2SQL(reader, "", 9999);
    }

    public void writeAll2SQL(String CSVFileSource) throws IOException {
        writeAll2SQL(new CSVReader(new FileReader(CSVFileSource)));
    }

    public void writeAll2SQL(CSVReader reader, String headerEncloser, int maxLineWidth) throws IOException {
        String[] array = reader.readNext();
        String types[] = new String[1024];
        for (int i = 0; i < array.length; i++) types[i] = "string";
        if(columnNames!=null) {
            for(int i=0;i<columnNames.length;i++)
                for(int j=0;j<array.length;j++)
                    if(columnNames[i].toUpperCase().equals(array[j].toUpperCase().trim())) {
                        array[j]=columnNames[i];
                        types[j]=columnTyes[i];
                    }
            columnNames=null;
        }
        columnTyes=types;
        init(array, headerEncloser, maxLineWidth);
        while ((array = reader.readNext()) != null) writeNextRow(array);
        close();
    }
}
