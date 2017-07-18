package com.opencsv;

import javax.xml.bind.DatatypeConverter;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by Tyler on 30/12/2016.
 */
public class ResultSetHelperService implements Closeable {
    // note: we want to maintain compatibility with Java 5 VM's
    // These types don't exist in Java 5
    public static int RESULT_FETCH_SIZE = 30000;
    public static int MAX_FETCH_ROWS = -1;
    public static boolean IS_TRIM = false;
    public static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public int columnCount;
    public String[] columnNames;
    public String[] columnTypes;
    public String[] columnClassName;
    public int[] columnTypesI;
    public Object[] rowObject;
    public long cost = 0;
    private SimpleDateFormat dateFormat;
    private SimpleDateFormat timeFormat;
    private DateTimeFormatter timeTZFormat;
    private ResultSet rs;
    private Object[] EOF = new Object[1];
    private ArrayBlockingQueue<Object[]> queue;
    private boolean isFinished = false;

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
        columnNames = new String[columnCount];
        columnTypes = new String[columnCount];
        columnClassName = new String[columnCount];
        columnTypesI = new int[columnCount];
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            int type = metadata.getColumnType(i + 1);
            String value;
            switch (type) {
                case Types.JAVA_OBJECT:
                    value = "object";
                    break;
                case Types.BOOLEAN:
                    value = "boolean";
                    break;
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.REAL:
                case Types.NUMERIC:
                    value = "double";
                    break;
                case Types.BIGINT:
                    value = "long";
                    break;
                case Types.BIT:
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    value = "int";
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
                    //case Types.TIME_WITH_TIMEZONE:
                    //case Types.TIMESTAMP_WITH_TIMEZONE:
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
                case Types.SQLXML:
                    value = "xml";
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
    public Object[] getColumnValues() throws SQLException, IOException {
        return this.getColumnValues(IS_TRIM, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     *
     * @param trim - values should have white spaces trimmed.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public Object[] getColumnValues(boolean trim) throws SQLException, IOException {
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
    public Object[] getColumnValues(boolean trim, String dateFormatString, String timeFormatString) throws SQLException, IOException {
        long sec = System.nanoTime();
        if (rs == null || rs.isClosed() || !rs.next()) {
            if (rs != null) rs.close();
            cost += System.nanoTime() - sec;
            isFinished = true;
            return null;
        }
        if (rowObject == null) rowObject = new Object[columnCount];
        Object o;
        for (int i = 0; i < columnCount; i++) {
            if (columnClassName[i] == null) {
                o = rs.getObject(i + 1);
                if (o != null) columnClassName[i] = o.getClass().getName();
            }
            switch (columnTypes[i]) {
                case "timestamptz":
                    try {
                        o = rs.getObject(i + 1, ZonedDateTime.class);
                    } catch (NullPointerException ex) {
                        o = rs.getObject(i + 1, OffsetDateTime.class);
                    }
                    break;
                case "timestamp":
                    o = rs.getObject(i + 1, Timestamp.class);
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
                case "xml":
                    SQLXML x = rs.getSQLXML(i + 1);
                    try {

                        if (x != null) o = x.getString();
                        else o = null;
                    } catch (Exception e) {
                        o = rs.getObject(i + 1);
                    }
                    break;
                default:
                    o = rs.getObject(i + 1);
            }
            if (o != null && rs.wasNull()) o = null;
            if (queue == null) rowObject[i] = getColumnValue(o, i, trim, dateFormatString, timeFormatString);
            else rowObject[i] = o;
        }
        cost += System.nanoTime() - sec;
        return rowObject;
    }

    @Override
    public void close() {
        try {
            if (rs != null && !rs.isClosed()) rs.close();
        } catch (Exception e) {
        }
        queue = null;
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
        String result = null;
        if (timeFormat == null) {
            DEFAULT_TIMESTAMP_FORMAT = timestampFormatString;
            timeFormat = new SimpleDateFormat(timestampFormatString);
        }
        if (timestamp != null) {
            result = timeFormat.format(timestamp);
            if (result.endsWith(".0")) result = result.substring(0, result.length() - 2);
        }
        return result;
    }

    protected String handleTimestampTZ(Object timestamp, String timestampFormatString) {
        if (timeTZFormat == null) {
            timeTZFormat = DateTimeFormatter.ofPattern(timestampFormatString + " X");
        }
        if (timestamp == null) return null;

        if (timestamp instanceof OffsetDateTime) return ((OffsetDateTime) timestamp).format(timeTZFormat);
        return ((ZonedDateTime) timestamp).format(timeTZFormat);
    }

    private Object getColumnValue(Object o, int colIndex, boolean trim, String dateFormatString, String timestampFormatString) throws SQLException, IOException {
        if (o == null) return null;
        String str;
        switch (columnTypes[colIndex]) {
            case "object":
                str = handleObject(o);
                break;
            case "boolean":
                return o;
            case "int":
                return ((Number) o).intValue();
            case "double":
            case "long":
                Double d = ((Number) o).doubleValue();
                switch (o.getClass().getSimpleName()) {
                    case "Double":
                        return o;
                    case "Float":
                        return Double.valueOf(o.toString());
                    case "BigInteger":
                        Integer i = ((BigInteger) o).intValue();
                        if (o.toString().equals(new BigInteger(d.toString()))) return i;
                        if (o.toString().equals(new BigInteger(d.toString()))) return d;
                        return o;
                    case "BigDecimal":
                        if (o.toString().equals(new BigDecimal(d).toString())) return d;
                        return o;
                    default:
                        return d;
                }
            case "date":
            case "time":
                str = handleDate((Date) o, dateFormatString);
                break;
            case "timestamp":
                try {
                    str = handleTimestamp((Timestamp) o, timestampFormatString);
                    if (columnClassName[colIndex].startsWith("oracle.sql.DATE")) {
                        int pos = str.lastIndexOf('.');
                        if (pos > 0) str = str.substring(0, pos - 1);
                    }
                } catch (Exception e) {
                    System.out.println(o + ":" + colIndex + ":" + columnClassName[colIndex] + ":" + columnNames[colIndex]);
                    throw e;
                }
                break;
            case "timestamptz":
                str = handleTimestampTZ(o, timestampFormatString);
                break;
            case "longraw":
                str = DatatypeConverter.printHexBinary((byte[]) o);
                break;
            default:
                str = o.toString();
        }
        return trim ? str.trim() : str;
    }

    public void startAsyncFetch(final RowCallback callback, final boolean trim, final String dateFormatString, final String timeFormatString, int fetchRows) throws Exception {
        if (fetchRows == -1) queue = new ArrayBlockingQueue<>(RESULT_FETCH_SIZE * 2 + 10);
        if (fetchRows > 0) {
            int size = Math.max(200, Math.min(fetchRows, RESULT_FETCH_SIZE));
            rs.getStatement().setMaxRows(fetchRows);
            if (size >= fetchRows / 2 && size < fetchRows) rs.setFetchSize(fetchRows / 2 + 1);
            else rs.setFetchSize(size);
            queue = new ArrayBlockingQueue<>(rs.getFetchSize() * 2 + 10);
        } else queue = new ArrayBlockingQueue<>(200);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (queue != null && !isFinished && (rowObject = new Object[columnCount]) != null)
                        queue.put(getColumnValues() == null ? EOF : rowObject);
                } catch (NullPointerException e0) {
                } catch (Exception e) {
                    try {
                        if (queue != null) queue.put(EOF);
                    } catch (Exception e1) {
                    }
                    if (e.getMessage().toLowerCase().indexOf("closed") == -1) e.printStackTrace();
                }
            }
        });
        t.setDaemon(true);
        t.start();

        Object[] values;
        int count = 0;
        while ((values = queue.take()) != EOF && (fetchRows < 0 || count++ < fetchRows)) {
            for (int i = 0; i < columnCount; i++)
                values[i] = getColumnValue(values[i], i, trim, dateFormatString, timeFormatString);
            callback.execute(values);
        }
        queue = null;
        t.join();
    }

    public void startAsyncFetch(final RowCallback c, boolean trim) throws Exception {
        startAsyncFetch(c, trim, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT, MAX_FETCH_ROWS);
    }

    public void startAsyncFetch(final RowCallback c) throws Exception {
        startAsyncFetch(c, IS_TRIM);
    }

    public Object[][] fetchRowsAsync(int rows) throws Exception {
        final ArrayList<Object[]> ary = new ArrayList();
        //ary.add(columnNames);
        startAsyncFetch(new RowCallback() {
            @Override
            public void execute(Object[] row) throws Exception {
                ary.add(row.clone());
            }
        }, false, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT, rows);
        return ary.toArray(new Object[][]{});
    }

    public Object[][] fetchRows(int rows) throws Exception {
        queue = null;
        final ArrayList<Object[]> ary = new ArrayList();
        Object[] row;
        int counter = 0;
        if(rows>0) rs.setFetchSize(rows<=1000?rows:rows/2);
        while (rows < 0 || ++counter <= rows) {
            row = getColumnValues();
            if (row == null) break;
            ary.add(row.clone());
        }
        return ary.toArray(new Object[][]{});
    }
}