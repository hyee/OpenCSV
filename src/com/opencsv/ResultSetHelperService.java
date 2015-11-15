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

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * helper class for processing JDBC ResultSet objects.
 */
public class ResultSetHelperService {

    // note: we want to maintain compatibility with Java 5 VM's
    // These types don't exist in Java 5

    public static int RESULT_FETCH_SIZE = 30000;
    public static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    public int columnCount;
    public String[] columnNames;
    public String[] columnTypes;
    public int[] columnTypesI;
    public String[] rowValue;
    public Object[] rowObject;
    public long cost = 0;
    private SimpleDateFormat dateFormat;
    private SimpleDateFormat timeFormat;
    private SimpleDateFormat timeTZFormat;
    private ResultSet rs;

    private ArrayBlockingQueue<Object[]> queue;
    private Object[] EOF = new Object[1];
    private boolean isFinished;

    /**
     * Default Constructor.
     */
    public ResultSetHelperService(ResultSet res, int fetchSize) throws SQLException {
        long sec = System.nanoTime();
        rs = res;
        rs.setFetchSize(fetchSize);
        RESULT_FETCH_SIZE = fetchSize;
        try {
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        } catch (Exception e) {
        }
        ResultSetMetaData metadata = rs.getMetaData();
        columnCount = metadata.getColumnCount();
        rowValue = new String[columnCount];
        isFinished = false;
        columnNames = new String[columnCount];
        columnTypes = new String[columnCount];
        columnTypesI = new int[columnCount];
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            int type = metadata.getColumnType(i + 1);
            String value;
            switch (type) {
                case Types.BIT:
                case Types.JAVA_OBJECT:
                    value = "object";
                    break;
                case Types.BOOLEAN:
                    value = "boolean";
                    break;
                case Types.BIGINT:
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.REAL:
                case Types.NUMERIC:
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    value = "number";
                    break;
                case Types.TIME:
                    value = "date";
                    break;
                case Types.DATE:
                    value = "date";
                    break;
                case Types.TIMESTAMP:
                case -100:
                    value = "timestamp";
                    break;
                case -101:
                case -102:
                    value = "timestamptz";
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    value = "raw";
                    break;
                case Types.CLOB:
                case Types.NCLOB:
                    value = "clob";
                    break;
                case Types.BLOB:
                    value = "blob";
                    break;
                default:
                    value = "string";
            }
            columnTypesI[i] = type;
            columnTypes[i] = value.intern();
            columnNames[i] = metadata.getColumnName(i + 1).intern();
        }
        cost += System.nanoTime() - sec;
    }

    public ResultSetHelperService(ResultSet res) throws SQLException {
        this(res, RESULT_FETCH_SIZE);
    }


    /**
     * Get all the column values from the result set.
     *
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public String[] getColumnValues() throws SQLException, IOException {
        return this.getColumnValues(true, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     *
     * @param trim - values should have white spaces trimmed.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public String[] getColumnValues(boolean trim) throws SQLException, IOException {
        return this.getColumnValues(trim, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     *
     * @param trim             - values should have white spaces trimmed.
     * @param dateFormatString - format String for dates.
     * @param timeFormatString - format String for timestamps.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public String[] getColumnValues(boolean trim, String dateFormatString, String timeFormatString) throws SQLException, IOException {
        long sec = System.nanoTime();
        if (!rs.next()) {
            rs.close();
            isFinished = true;
            return null;
        }
        Object o;
        for (int i = 0; i < columnCount; i++) {
            switch (columnTypes[i]) {
                case "timestamptz":
                    o = rs.getTimestamp(i + 1);
                    break;
                case "raw":
                    o = rs.getString(i + 1);
                    break;
                case "blob":
                    Blob bl = rs.getBlob(i + 1);
                    if (bl != null) {
                        o = DatatypeConverter.printHexBinary(bl.getBytes(1, (int) bl.length()));
                        bl.free();
                    } else o = null;
                    break;
                case "clob":
                    Clob c = rs.getClob(i + 1);
                    if (c != null) {
                        o = c.getSubString(1, (int) c.length());
                        c.free();
                    } else o = null;
                    break;
                default:
                    o = rs.getObject(i + 1);
            }
            if (o != null && rs.wasNull()) o = null;

            if (queue == null)
                rowValue[i] = getColumnValue(o, columnTypes[i], trim, dateFormatString, timeFormatString);
            else rowObject[i] = o;
        }
        cost += System.nanoTime() - sec;
        return rowValue;
    }


    /**
     * changes an object to a String.
     *
     * @param obj - Object to format.
     * @return - String value of an object or empty string if the object is null.
     */
    protected String handleObject(Object obj) {
        return obj == null ? "" : String.valueOf(obj);
    }


    protected String handleDate(Date date, String dateFormatString) {
        if (dateFormat == null) {
            DEFAULT_DATE_FORMAT = dateFormatString;
            dateFormat = new SimpleDateFormat(dateFormatString);
        }
        return date == null ? null : dateFormat.format(date);
    }

    protected String handleTimestamp(Timestamp timestamp, String timestampFormatString) {
        if (timeFormat == null) {
            DEFAULT_TIMESTAMP_FORMAT = timestampFormatString;
            timeFormat = new SimpleDateFormat(timestampFormatString);
        }
        return timestamp == null ? null : timeFormat.format(timestamp);
    }

    protected String handleTimestampTZ(Timestamp timestamp, String timestampFormatString) {
        if (timeFormat == null) {
            timeTZFormat = new SimpleDateFormat(timestampFormatString + " S");
        }
        return timestamp == null ? null : timeTZFormat.format(timestamp);
    }

    private String getColumnValue(Object o, String colType, boolean trim, String dateFormatString, String timestampFormatString) throws SQLException, IOException {
        if (o == null) return "";
        String str;
        switch (colType) {
            case "object":
                str = handleObject(o);
                break;
            case "boolean":
                boolean b = (Boolean) o;
                str = Boolean.valueOf(b).toString();
                break;
            case "date":
            case "time":
                str = handleDate((Date) o, dateFormatString);
                break;
            case "timestamp":
                str = handleTimestamp((Timestamp) o, timestampFormatString);
                break;
            case "timestamptz":
                str = handleTimestampTZ((Timestamp) o, timestampFormatString);
                break;
            case "longraw":
                str = DatatypeConverter.printHexBinary((byte[]) o);
                break;
            default:
                str = o.toString();
        }
        return trim ? str.trim() : str;
    }

    public void startAsyncFetch(final RowCallback c, final boolean trim, final String dateFormatString, final String timeFormatString, int fetchRows) throws Exception {
        if (fetchRows < 0) queue = new ArrayBlockingQueue<>(RESULT_FETCH_SIZE * 2 + 10);
        else {
            queue = new ArrayBlockingQueue<>(fetchRows);
            rs.setFetchSize(fetchRows);
            rs.getStatement().setMaxRows(fetchRows);
        }
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (queue != null && !isFinished && (rowObject = new Object[columnCount]) != null)
                        queue.put(getColumnValues() == null ? EOF : rowObject);
                } catch (Exception e) {
                    if(e.getMessage().toLowerCase().indexOf("closed")==-1) e.printStackTrace();
                }
            }
        });
        t.setDaemon(true);
        t.start();


        Object[] values;
        int count = 0;
        while ((values = queue.take()) != EOF && (fetchRows < 0 || count++ < fetchRows)) {
            for (int i = 0; i < columnCount; i++)
                values[i] = getColumnValue(values[i], columnTypes[i], trim, dateFormatString, timeFormatString);
            c.execute(values);
        }
        queue = null;
        t.join();
    }

    public ArrayList<Object[]> fetchRows(int rows) throws Exception {
        final ArrayList<Object[]> ary = new ArrayList();
        //ary.add(columnNames);
        startAsyncFetch(new RowCallback() {
            @Override
            public void execute(Object[] row) throws Exception {
                ary.add(row);
            }
        }, false, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT, rows);
        return ary;
    }

    public void startAsyncFetch(final RowCallback c, boolean trim) throws Exception {
        startAsyncFetch(c, trim, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT, -1);
    }

    public void startAsyncFetch(final RowCallback c) throws Exception {
        startAsyncFetch(c, true);
    }
}