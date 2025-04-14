package com.opencsv;

import javax.xml.bind.DatatypeConverter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
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
    public static int RESULT_FETCH_SIZE = 10000;
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
    private SimpleDateFormat TimeTZFormat1;
    private DateTimeFormatter timeTZFormat;
    private final ResultSet rs;
    private final Object[] EOF = new Object[1];
    private ArrayBlockingQueue<Object[]> queue;
    private boolean isFinished = false;
    private static volatile boolean isAborted = false;
    private final StringBuilder sb = new StringBuilder();

    /**
     * Default Constructor.
     */
    public ResultSetHelperService(ResultSet res, int fetchSize) throws SQLException {
        long sec = System.nanoTime();
        rs = res;
        rs.setFetchSize(fetchSize);

        RESULT_FETCH_SIZE = fetchSize;
        isAborted = false;
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
                case Types.BIT:
                case Types.BOOLEAN:
                case 252: //PLSQL_BOOLEAN
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
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    value = "int";
                    break;
                case Types.TIME:
                    value = "time";
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
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    //case Types.TIME_WITH_TIMEZONE:
                    value = "timestamptz";
                    break;
                case -105:
                case -106:
                case -107:
                case -108:
                    value = "vector";
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    value = "raw";
                    break;
                case Types.CLOB:
                case Types.NCLOB:
                    //case 2016: //JSON
                    value = "clob";
                    break;
                case Types.BLOB:
                    value = "blob";
                    break;
                case Types.SQLXML:
                    value = "xml";
                    break;
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                    value = "longtext";
                    break;
                case Types.ARRAY:
                    value = "array";
                    break;
                case Types.STRUCT:
                case 2008:
                    value = "struct";
                    break;
                case 2007:
                    value = "anydata";
                    break;
                case 2016:
                    value = "oraclejson";
                    break;
                default:
                    value = "string";
            }

            columnTypesI[i] = type;
            columnTypes[i] = value.intern();
            final String name = metadata.getColumnName(i + 1);
            //System.out.println(name+":"+type);
            columnNames[i] = (name == null ? "" : name).intern();
        }
        cost += System.nanoTime() - sec;
    }

    public static void abort() {
        isAborted = true;
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
        if (isAborted) {
            if (rs != null) rs.close();
            throw new IOException("Processing is aborted!");
        }
        if (rs == null || rs.isClosed() || !rs.next()) {
            if (rs != null) rs.close();
            cost += System.nanoTime() - sec;
            isFinished = true;
            return null;
        }
        if (rowObject == null) rowObject = new Object[columnCount];
        Object o = null;
        boolean isFetched;
        for (int i = 0; i < columnCount; i++) {
            isFetched = false;
            if (columnClassName[i] == null) {
                try {
                    o = rs.getObject(i + 1);
                } catch (SQLException e) {
                    o = null;
                }
                if (o != null) columnClassName[i] = o.getClass().getName();
                isFetched = true;
            }
            switch (columnTypes[i]) {
                case "timestamptz":
                    try {
                        o = rs.getObject(i + 1, ZonedDateTime.class);
                    } catch (AbstractMethodError ex2) {
                        o = rs.getTimestamp(i + 1);
                    } catch (Exception ex) {
                        try {
                            o = rs.getObject(i + 1, OffsetDateTime.class);
                        } catch (Exception ex1) {
                            o = rs.getTimestamp(i + 1);
                        }
                    }
                    break;
                case "timestamp":
                    o = rs.getTimestamp(i + 1);
                    break;
                case "raw":
                    o = rs.getString(i + 1);
                    break;
                case "blob":
                    Blob bl = rs.getBlob(i + 1);
                    if (bl != null) {
                        o = DatatypeConverter.printHexBinary(bl.getBytes(1, (int) bl.length()));
                        try {
                            bl.free();
                        } catch (Exception e) {
                        }
                    } else o = null;
                    break;
                case "clob":
                    Clob c = rs.getClob(i + 1);
                    if (c != null) {
                        o = c.getSubString(1, (int) c.length());
                        try {
                            c.free();
                        } catch (Exception e) {
                        }
                    } else o = null;
                    break;
                case "xml":
                    SQLXML x = rs.getSQLXML(i + 1);
                    try {
                        if (x != null) o = x.getString();
                        else o = null;
                    } catch (Exception e) {
                        o = isFetched ? o : rs.getObject(i + 1);
                    }
                    break;
                case "oraclejson":
                    try {
                        o = rs.getObject(i + 1, Class.forName("oracle.sql.json.OracleJsonValue"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "vector":
                    try {
                        o = rs.getObject(i + 1, Class.forName("oracle.sql.VECTOR"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    try {
                        o = isFetched ? o : rs.getObject(i + 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        o = null;
                    }
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
            else if (result.endsWith(".000")) result = result.substring(0, result.length() - 4);
        }
        return result;
    }

    protected String handleTimestampTZ(Object timestamp, String timestampFormatString) {
        if (timeTZFormat == null) {
            timeTZFormat = DateTimeFormatter.ofPattern(timestampFormatString + " X");
            TimeTZFormat1 = new SimpleDateFormat(timestampFormatString + " X");
        }
        if (timestamp == null) return null;
        if (timestamp instanceof OffsetDateTime) return ((OffsetDateTime) timestamp).format(timeTZFormat);
        else if (timestamp instanceof Timestamp) return TimeTZFormat1.format((Timestamp) timestamp);
        return ((ZonedDateTime) timestamp).format(timeTZFormat);
    }

    public void object2String(StringBuilder sb, Object obj, String indent, String dateFormatString, String timestampFormatString) throws Exception {
        Object[] arr;
        if (obj instanceof Array) {
            sb.append('{');
            arr = (Object[]) ((Array) obj).getArray();
        } else {
            arr = ((Struct) obj).getAttributes();
            sb.append(((Struct) obj).getSQLTypeName()).append("(");
        }
        indent = indent + "  ";
        for (int index = 0; index < arr.length; index++) {
            final Object item = arr[index];
            if (index > 0) sb.append(",");
            if (item == null) {
                sb.append("null");
            } else if (item instanceof Struct || item instanceof Array) {
                sb.append("\n").append(indent);
                object2String(sb, item, indent, dateFormatString, timestampFormatString);
            } else {
                String str = item.toString();
                if (item instanceof Number) {
                    BigDecimal number;
                    if (item instanceof BigDecimal) {
                        number = (BigDecimal) item;
                        number.setScale(10);
                    } else if (item instanceof BigInteger) {
                        number = new BigDecimal((BigInteger) item);
                    } else if (item instanceof Long) {
                        number = BigDecimal.valueOf((Long) item);
                    } else {
                        number = BigDecimal.valueOf(((Number) item).doubleValue());
                        number.setScale(10);
                    }
                    sb.append(number.stripTrailingZeros().toPlainString());
                } else if (item instanceof Timestamp) {
                    sb.append("'").append(handleTimestamp((Timestamp) item, timestampFormatString)).append("'");
                } else if (item instanceof Date) {
                    sb.append("'").append(handleDate((Date) item, dateFormatString)).append("'");
                } else {
                    sb.append("'").append(str.replace("'", "''")).append("'");
                }
            }
        }

        if (obj instanceof Array) {
            sb.append('}');
        } else {
            sb.append(')');
        }
    }

    private Object getColumnValue(Object o, int colIndex, boolean trim, String dateFormatString, String timestampFormatString) throws SQLException, IOException {
        if (o == null) return null;
        String str;
        final double d;
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
                str = o.getClass().getSimpleName();
                if (str.equals(columnTypes[colIndex])) str = Character.toUpperCase(str.charAt(0)) + str.substring(1);
                columnTypes[colIndex] = str;
                return getColumnValue(o, colIndex, trim, dateFormatString, timestampFormatString);
            case "Double":
                d = ((Number) o).doubleValue();
                return d == (long) d ? (long) d : d;
            case "Float":
                return Double.valueOf(o.toString());
            case "Integer":
                return ((Number) o).intValue();
            case "Long":
                return ((Number) o).longValue();
            case "BigInteger":
                if (o instanceof BigInteger) return o.toString();
                d = ((Number) o).doubleValue();
                if (o.toString().equals(new BigInteger(String.valueOf((long) d)))) return (long) d;
                return o;
            case "BigDecimal":
                if (o instanceof BigDecimal) return o.toString();
                d = ((Number) o).doubleValue();
                final long l = (long) d;
                if (d == l && o.toString().equals(BigInteger.valueOf(l))) return l;
                if (o.toString().equals(BigDecimal.valueOf(d).toString())) return d;
                return o.toString();
            case "date":
                str = handleDate((Date) o, dateFormatString);
                break;
            case "time":
                str = o.toString();
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
            case "array":
            case "struct":
                try {
                    if (!o.getClass().getTypeName().contains("postgres") && (o instanceof Array || o instanceof Struct)) {
                        sb.setLength(0);
                        object2String(sb, o, "", dateFormatString, timestampFormatString);
                        str = sb.toString();
                    } else {
                        str = o.toString();
                    }
                } catch (Exception e) {
                    str = o.toString();
                }
                break;
            case "anydata":
                try {
                    if (!(o instanceof String) && !(o instanceof Number)) {
                        Method method = o.getClass().getDeclaredMethod("stringValue");
                        str = (String) method.invoke(o);
                    } else {
                        str = o.toString();
                    }
                } catch (Exception e) {
                    str = o.toString();
                }
                break;
            case "vector":
                try {
                    Method method = o.getClass().getDeclaredMethod("toDoubleArray");
                    final double[] ary = (double[]) method.invoke(o);
                    sb.setLength(0);
                    sb.append(('['));
                    final int total = ary.length;
                    for (int i = 0; i < total; i++) {
                        sb.append(ary[i]);
                        if((i + 1) < total) {
                            sb.append(((i + 1) % 4 == 0)?",\n ":",");
                        }
                    }
                    sb.append(']');
                    str = sb.toString();
                } catch (Exception e) {
                    str = o.toString();
                }
                break;
            default:
                if (o instanceof Number) {
                    d = ((Number) o).doubleValue();
                    return d == (long) d ? (long) d : d;
                }
                str = o.toString();
        }
        return trim ? str.trim() : str;
    }

    private volatile Exception exception = null;

    public void startAsyncFetch(final RowCallback callback, final boolean trim, final String dateFormatString, final String timeFormatString, int fetchRows) throws Exception {
        if (fetchRows == -1) queue = new ArrayBlockingQueue<>(RESULT_FETCH_SIZE * 2 + 10);
        else if (fetchRows > 0) {
            int size = Math.max(256, Math.min(fetchRows, RESULT_FETCH_SIZE));
            rs.getStatement().setMaxRows(fetchRows);
            if (size >= fetchRows / 2 && size < fetchRows) rs.setFetchSize(fetchRows / 2 + 1);
            else rs.setFetchSize(size);
            queue = new ArrayBlockingQueue<>(rs.getFetchSize() * 2 + 10);
        } else queue = new ArrayBlockingQueue<>(256);
        exception = null;
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
                    exception = e;
                }
            }
        });
        t.setDaemon(true);
        t.start();

        Object[] values;
        int count = 0;
        while ((values = queue.take()) != EOF && (fetchRows < 0 || ++count < fetchRows)) {
            for (int i = 0; i < columnCount; i++)
                values[i] = getColumnValue(values[i], i, trim, dateFormatString, timeFormatString);
            callback.execute(values);
        }
        queue = null;
        t.join();
        if (exception != null) throw exception;
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
        startAsyncFetch(row -> ary.add(row.clone()), false, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT, rows);
        return ary.toArray(new Object[][]{});
    }

    public Object[][] fetchRows(int rows) throws Exception {
        queue = null;
        final ArrayList<Object[]> ary = new ArrayList();
        Object[] row;
        int counter = 0;
        if (rows > 0) rs.setFetchSize(rows <= 1000 ? rows : rows / 2);
        while (rows < 0 || ++counter <= rows) {
            row = getColumnValues();
            if (row == null) break;
            ary.add(row.clone());
        }
        return ary.toArray(new Object[][]{});
    }
}