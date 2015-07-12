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

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * helper class for processing JDBC ResultSet objects.
 */
public class ResultSetHelperService implements ResultSetHelper {
    public static final int CLOBBUFFERSIZE = 2048;

    // note: we want to maintain compatibility with Java 5 VM's
    // These types don't exist in Java 5

    static final int BLOB_MAX_SIZE = 32767;
    static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    static final String DEFAULT_TIMESTAMPTZ_FORMAT = "yyyy-MM-dd HH:mm:ss.S X";
    private SimpleDateFormat dateFormat;
    private SimpleDateFormat timeFormat;
    private SimpleDateFormat timeTZFormat;
    private StringBuilder stringBuilder = new StringBuilder(32767);
    private HashMap<ResultSet, Integer[]> map = new HashMap<ResultSet, Integer[]>();

    /**
     * Default Constructor.
     */
    public ResultSetHelperService() {
    }


    /**
     * Returns the column names from the result set.
     *
     * @param rs - ResultSet
     * @return - a string array containing the column names.
     * @throws SQLException - thrown by the result set.
     */
    public String[] getColumnNames(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<String>();
        ResultSetMetaData metadata = rs.getMetaData();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            names.add(metadata.getColumnName(i + 1));
        }

        String[] nameArray = new String[names.size()];
        return names.toArray(nameArray);
    }

    public String[] getColumnTypes(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<String>();
        List<Integer> types = new ArrayList<Integer>();
        ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            String value;
            int type = metadata.getColumnType(i + 1);
            switch (type) {
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
                default:
                    value = "string";
            }
            names.add(value.intern());
            types.add(type);
        }

        String[] nameArray = new String[names.size()];
        Integer[] typeArray = new Integer[types.size()];
        map.put(rs, types.toArray(typeArray));
        return names.toArray(nameArray);
    }

    /**
     * Get all the column values from the result set.
     *
     * @param rs - the ResultSet containing the values.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public String[] getColumnValues(ResultSet rs) throws SQLException, IOException {
        return this.getColumnValues(rs, true, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     *
     * @param rs   - the ResultSet containing the values.
     * @param trim - values should have white spaces trimmed.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public String[] getColumnValues(ResultSet rs, boolean trim) throws SQLException, IOException {
        return this.getColumnValues(rs, trim, DEFAULT_DATE_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    }

    /**
     * Get all the column values from the result set.
     *
     * @param rs               - the ResultSet containing the values.
     * @param trim             - values should have white spaces trimmed.
     * @param dateFormatString - format String for dates.
     * @param timeFormatString - format String for timestamps.
     * @return - String array containing all the column values.
     * @throws SQLException - thrown by the result set.
     * @throws IOException  - thrown by the result set.
     */
    public String[] getColumnValues(ResultSet rs, boolean trim, String dateFormatString, String timeFormatString) throws SQLException, IOException {
        List<String> values = new ArrayList<String>();
        if (!map.containsKey(rs)) getColumnTypes(rs);
        Integer[] columnTypes = map.get(rs);
        for (int i = 0; i < columnTypes.length; i++) {
            values.add(getColumnValue(rs, columnTypes[i], i + 1, trim, dateFormatString, timeFormatString));
        }
        String[] valueArray = new String[values.size()];
        return values.toArray(valueArray);
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


    protected String handleDate(ResultSet rs, int columnIndex, String dateFormatString) throws SQLException {
        java.sql.Date date = rs.getDate(columnIndex);
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat(dateFormatString);
        }
        return date == null ? null : dateFormat.format(date);
    }


    protected String handleTimestamp(ResultSet rs, int columnIndex, String timestampFormatString) throws SQLException {
        java.sql.Timestamp timestamp = rs.getTimestamp(columnIndex);
        if (timeFormat == null) {
            timeFormat = new SimpleDateFormat(timestampFormatString);
        }
        return timestamp == null ? null : timeFormat.format(timestamp);
    }

    protected String handleTimestampTZ(ResultSet rs, int columnIndex, String timestampFormatString) throws SQLException {
        java.sql.Timestamp timestamp = rs.getTimestamp(columnIndex);
        if (timeFormat == null) {
            timeTZFormat = new SimpleDateFormat(DEFAULT_TIMESTAMPTZ_FORMAT);
        }
        return timestamp == null ? null : timeTZFormat.format(timestamp);
    }

    private String getColumnValue(ResultSet rs, int colType, int colIndex, boolean trim, String dateFormatString, String timestampFormatString) throws SQLException, IOException {
        String value = "";
        switch (colType) {
            case Types.BIT:
            case Types.JAVA_OBJECT:
                value = handleObject(rs.getObject(colIndex));
                break;
            case Types.BOOLEAN:
                boolean b = rs.getBoolean(colIndex);
                value = Boolean.valueOf(b).toString();
                break;
            case Types.BLOB:
                Blob bl = rs.getBlob(colIndex);
                if (bl != null) {
                    int len = (int) bl.length();
                    byte[] src = bl.getBytes(1, len > BLOB_MAX_SIZE ? BLOB_MAX_SIZE : len);
                    bl.free();
                    for (int i = 0; i < src.length; i++) {
                        int v = src[i] & 0xFF;
                        String hv = Integer.toHexString(v);
                        if (hv.length() < 2) {
                            stringBuilder.append(0);
                        }
                        stringBuilder.append(hv);
                    }
                    value = stringBuilder.toString().toLowerCase();
                    stringBuilder.setLength(0);
                }
                break;
            case Types.NCLOB:
            case Types.CLOB:
                Clob c = rs.getClob(colIndex);
                if (c != null) {
                    value = c.getSubString(1, (int) c.length());
                    c.free();
                }
                break;
            case Types.DATE:
            case Types.TIME:
                value = handleDate(rs, colIndex, dateFormatString);
                break;
            case Types.TIMESTAMP:
            case -100:
                value = handleTimestamp(rs, colIndex, timestampFormatString);
                break;
            case -101:
            case -102:
                value = handleTimestampTZ(rs, colIndex, timestampFormatString);
                break;
            default:
                value = rs.getString(colIndex);
        }

        if (value == null) {
            value = "";
        }

        return trim ? value.trim() : value;
    }
}
