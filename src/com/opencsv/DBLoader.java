package com.opencsv;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.*;
import java.util.Date;
import java.util.stream.Stream;

/**
 * DBLoader - A robust, database-agnostic class for loading CSV data into database tables.
 * <p>
 * This class uses PreparedStatement with batch processing to insert data, making it compatible with
 * Oracle, MySQL, PostgreSQL, MSSQL, and other JDBC-compliant databases.
 * <p>
 * Features:
 * - Automatic column mapping (case-insensitive) when CSV has headers
 * - Intelligent data type conversion (numeric, date, timestamp, boolean, string)
 * - Batch processing for efficient inserts using PreparedStatement
 * - Progress tracking with configurable intervals
 * - Error handling with .bad file output for failed rows
 * - Transaction management with commit/rollback
 * - Database-specific column quoting (MySQL, PostgreSQL, SQL Server, Oracle)
 * - Comprehensive configuration via {@link DBLoaderConfig} class
 * <p>
 * Example usage with DBLoaderConfig:
 * <pre>
 * Connection conn = DriverManager.getConnection(url, user, password);
 * DBLoaderConfig config = new DBLoaderConfig();
 * config.batchSize = 1000;
 * config.create = true;
 * DBLoader loader = new DBLoader(conn, "my_table", "data.csv", config);
 * long rowsLoaded = loader.load();
 * System.out.println("Loaded " + rowsLoaded + " rows");
 * </pre>
 * <p>
 * Example usage with options map:
 * <pre>
 * Connection conn = DriverManager.getConnection(url, user, password);
 * Map<String, Object> options = new HashMap<>();
 * options.put(DBLoaderConfig.BATCH_ROWS, 1000);
 * options.put(DBLoaderConfig.CREATE, true);
 * long rowsLoaded = DBLoader.importCSVData(conn, "my_table", "data.csv", options);
 * System.out.println("Loaded " + rowsLoaded + " rows");
 * </pre>
 */
public class DBLoader {
    private static final int MAX_BLOB_SIZE = 10 * 1024 * 1024;
    private static final Map<String, DateTimeFormatter> DATETIME_FORMATTER_CACHE = new HashMap<>();
    private static final Map<String, DateTimeFormatter> TIME_FORMATTER_CACHE = new HashMap<>();

    static {
        final String[] orgStrings = {
                "yyyy-MM-dd",
                "MM-dd-yyyy",
                "dd-MM-yyyy",
                "yyyy/MM/dd",
                "MM/dd/yyyy",
                "dd/MM/yyyy",
                "yyyyMMdd"};

        final String[] DateStrings =
                Stream.concat(
                                Stream.of(orgStrings),
                                Stream.of(orgStrings).map(s -> s.replace("MM", "MMM")))
                        .distinct().toArray(String[]::new);

        final String[] DateSeps = {
                " ", "T"
        };

        final String[] TimeStrings = {
                "HH:mm:ss",
                "hh:mm:ss a"
        };

        final String[] zoneSeps = {
                "", " "
        };

        final String[] zoneStrings = {
                "XXX", "Z"
        };

        DateTimeFormatter formatter;
        for (String dateString : DateStrings) {
            for (String zoneSep : zoneSeps) {
                for (String zoneString : zoneStrings) {
                    //DateStrings without time
                    formatter = new DateTimeFormatterBuilder()
                            .appendPattern(dateString)
                            .optionalStart()
                            .appendLiteral(zoneSep)
                            .appendPattern(zoneString)
                            .optionalEnd()
                            .toFormatter()
                            .withResolverStyle(ResolverStyle.LENIENT);
                    DATETIME_FORMATTER_CACHE.put(dateString + zoneSep + zoneString, formatter);
                    //DateStrings with time
                    for (String dateSep : DateSeps) {
                        for (String timeString : TimeStrings) {
                            formatter = new DateTimeFormatterBuilder()
                                    .appendPattern(dateString)
                                    .optionalStart()
                                    .appendLiteral(dateSep)
                                    .appendPattern(timeString)
                                    .optionalStart()
                                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                                    .optionalStart()
                                    .appendLiteral(zoneSep)
                                    .appendPattern(zoneString)
                                    .optionalEnd()
                                    .optionalEnd()
                                    .optionalEnd()
                                    .toFormatter()
                                    .withResolverStyle(ResolverStyle.LENIENT);
                            DATETIME_FORMATTER_CACHE.put(dateString + dateSep + timeString + ".SSSSS" + zoneSep + zoneString, formatter);

                            //Without us
                            formatter = new DateTimeFormatterBuilder()
                                    .appendPattern(dateString)
                                    .optionalStart()
                                    .appendLiteral(dateSep)
                                    .appendPattern(timeString)
                                    .optionalStart()
                                    .appendLiteral(zoneSep)
                                    .appendPattern(zoneString)
                                    .optionalEnd()
                                    .optionalEnd()
                                    .toFormatter()
                                    .withResolverStyle(ResolverStyle.LENIENT);
                            DATETIME_FORMATTER_CACHE.put(dateString + dateSep + timeString + zoneSep + zoneString, formatter);
                        }
                    }
                }
            }
        }

        String dateString = "dd-MMM-";
        String name = "DD-MON-RR";
        for (String zoneSep : zoneSeps) {
            for (String zoneString : zoneStrings) {
                //DateStrings without time
                formatter = new DateTimeFormatterBuilder()
                        .appendPattern(dateString)
                        .appendValueReduced(ChronoField.YEAR, 2, 2, LocalDate.now().getYear() - 50)
                        .optionalStart()
                        .appendLiteral(zoneSep)
                        .appendPattern(zoneString)
                        .optionalEnd()
                        .toFormatter(Locale.ENGLISH)
                        .withResolverStyle(ResolverStyle.LENIENT);
                DATETIME_FORMATTER_CACHE.put(name + zoneSep + zoneString, formatter);
                //DateStrings with time
                for (String dateSep : DateSeps) {
                    for (String timeString : TimeStrings) {
                        //With us
                        formatter = new DateTimeFormatterBuilder()
                                .appendPattern(dateString)
                                .appendValueReduced(ChronoField.YEAR, 2, 2, LocalDate.now().getYear() - 50)
                                .optionalStart()
                                .appendLiteral(dateSep)
                                .appendPattern(timeString)
                                .optionalStart()
                                .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                                .optionalStart()
                                .appendLiteral(zoneSep)
                                .appendPattern(zoneString)
                                .optionalEnd()
                                .optionalEnd()
                                .optionalEnd()
                                .toFormatter(Locale.ENGLISH)
                                .withResolverStyle(ResolverStyle.LENIENT);
                        DATETIME_FORMATTER_CACHE.put(name + dateSep + timeString + ".SSSSS" + zoneSep + zoneString, formatter);

                        //Without us
                        formatter = new DateTimeFormatterBuilder()
                                .appendPattern(dateString)
                                .appendValueReduced(ChronoField.YEAR, 2, 2, LocalDate.now().getYear() - 50)
                                .optionalStart()
                                .appendLiteral(dateSep)
                                .appendPattern(timeString)
                                .optionalStart()
                                .appendLiteral(zoneSep)
                                .appendPattern(zoneString)
                                .optionalEnd()
                                .optionalEnd()
                                .toFormatter(Locale.ENGLISH)
                                .withResolverStyle(ResolverStyle.LENIENT);
                        DATETIME_FORMATTER_CACHE.put(name + dateSep + timeString + zoneSep + zoneString, formatter);
                    }
                }
            }
        }


        //TimeStrings
        for (String zoneSep : zoneSeps) {
            for (String zoneString : zoneStrings) {
                for (String timeString : TimeStrings) {
                    //With us
                    formatter = new DateTimeFormatterBuilder()
                            .appendPattern(timeString)
                            .optionalStart()
                            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                            .optionalStart()
                            .appendLiteral(zoneSep)
                            .appendPattern(zoneString)
                            .optionalEnd()
                            .optionalEnd()
                            .toFormatter()
                            .withResolverStyle(ResolverStyle.LENIENT);
                    TIME_FORMATTER_CACHE.put(timeString + ".SSSSS" + zoneSep + zoneString, formatter);

                    //Without us
                    formatter = new DateTimeFormatterBuilder()
                            .appendPattern("HH:mm:ss")
                            .optionalStart()
                            .appendLiteral(zoneSep)
                            .appendPattern(zoneString)
                            .optionalEnd()
                            .toFormatter()
                            .withResolverStyle(ResolverStyle.LENIENT);
                    TIME_FORMATTER_CACHE.put(timeString + zoneSep + zoneString, formatter);
                }
            }
        }
    }

    private Connection connection;
    private String tableName;
    private String csvFilePath;
    private DBLoaderConfig config;
    private long totalRowsProcessed;
    private long totalErrors;
    private long totalBytesProcessed;
    private long startTime;
    private long lastProgressTime;
    private long lastProgressRows;
    private long lastProgressBytes;
    private long lastProgressErrors;
    private CSVWriter badFileWriter;
    private String badFilePath;
    private boolean isNeedRebuildTimestampFormat = false;
    private int timestampFormatCount = 0;
    private boolean isNeedRebuildTimeFormat = false;
    private int timeFormatCount = 0;

    /**
     * Creates a DBLoader with full customization options using DBLoaderConfig.
     *
     * @param connection  the database connection
     * @param tableName   the target table name
     * @param csvFilePath the path to the CSV file
     * @param config      the configuration object containing all settings except csvFilePath
     */
    public DBLoader(Connection connection, String tableName, String csvFilePath, DBLoaderConfig config) {
        this.connection = connection;
        this.tableName = tableName;
        this.csvFilePath = csvFilePath;
        this.config = config;
    }


    /**
     * Creates a DBLoader with full customization options using DBLoaderConfig.
     *
     * @param connection  the database connection
     * @param tableName   the target table name
     * @param csvFilePath the path to the CSV file
     * @param options     a map of configuration options
     */
    public DBLoader(Connection connection, String tableName, String csvFilePath, Map<String, Object> options) {
        this(connection, tableName, csvFilePath, DBLoaderConfig.parseOptions(options));
    }

    /**
     * Quotes a column name according to database-specific syntax.
     * MySQL uses backticks, PostgreSQL uses double quotes,
     * SQL Server uses square brackets, Oracle uses double quotes.
     *
     * @param columnName          the column name to quote
     * @param databaseProductName the database product name (e.g., "MySQL", "PostgreSQL")
     * @return the quoted column name
     */
    private static String quoteColumnName(String columnName, String databaseProductName) {
        if (databaseProductName == null) {
            return columnName;
        }

        String upperProductName = databaseProductName.toUpperCase();

        if (upperProductName.contains("MYSQL")) {
            return "`" + columnName + "`";
        } else if (upperProductName.contains("POSTGRESQL")) {
            return "\"" + columnName + "\"";
        } else if (upperProductName.contains("SQL SERVER") || upperProductName.contains("SYBASE")) {
            return "[" + columnName + "]";
        } else if (upperProductName.contains("ORACLE")) {
            return "\"" + columnName + "\"";
        } else {
            return "\"" + columnName + "\"";
        }
    }

    /**
     * Checks if a string represents a valid numeric value.
     * Supports integers, decimals, and scientific notation.
     * Allows optional leading sign (+/-).
     *
     * @param str the string to check
     * @return the parsed numeric value, or null if not valid
     */
    private static Number parseNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }
        str = str.trim();
        if (str.isEmpty()) {
            return null;
        }
        
        // Optimized parsing: first check if it's a simple integer or decimal
        boolean isDecimal = false;
        boolean hasExponent = false;
        boolean hasSign = false;
        
        // Quick format check
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '.') {
                if (isDecimal) {
                    // Multiple decimal points, not a valid number
                    return null;
                }
                isDecimal = true;
            } else if (c == 'e' || c == 'E') {
                hasExponent = true;
                break; // Don't check exponent part further
            } else if (c == '+' || c == '-') {
                if (i != 0 && str.charAt(i - 1) != 'e' && str.charAt(i - 1) != 'E') {
                    // Sign not at beginning or after exponent, invalid
                    return null;
                }
                hasSign = true;
            } else if (!Character.isDigit(c)) {
                return null;
            }
        }
        
        try {
            // For simple integers without decimal or exponent, use faster parsing
            if (!isDecimal && !hasExponent) {
                // Try parsing as primitive long first for performance
                long longValue;
                try {
                    longValue = Long.parseLong(str);
                    // Convert to smallest possible integer type
                    if (longValue >= Byte.MIN_VALUE && longValue <= Byte.MAX_VALUE) {
                        return (byte) longValue;
                    } else if (longValue >= Short.MIN_VALUE && longValue <= Short.MAX_VALUE) {
                        return (short) longValue;
                    } else if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                        return (int) longValue;
                    } else {
                        return longValue;
                    }
                } catch (NumberFormatException e) {
                    // If too large for long, try BigInteger
                    return new BigInteger(str);
                }
            }
            
            // For decimals or numbers with exponent, use BigDecimal for accuracy
            BigDecimal num = new BigDecimal(str);
            
            // Try to convert to smaller types if possible
            try {
                return num.toBigIntegerExact();
            } catch (ArithmeticException e) {
                // Has decimal part, try double if no precision loss
                double doubleValue = num.doubleValue();
                if (Double.isFinite(doubleValue)&&BigDecimal.valueOf(doubleValue).compareTo(num) == 0) {
                    return doubleValue;
                }
                // If double can't represent accurately, return BigDecimal
                return num;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parses a boolean value from a string.
     * Supports various representations: TRUE, FALSE, 1, 0, YES, NO, Y, N (case-insensitive).
     *
     * @param value the string to parse
     * @return the boolean value
     */
    private static boolean parseBoolean(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        String upperValue = value.toUpperCase().trim();
        if (upperValue.equals("TRUE") || upperValue.equals("1") || upperValue.equals("YES") || upperValue.equals("Y")) {
            return true;
        }
        if (upperValue.equals("FALSE") || upperValue.equals("0") || upperValue.equals("NO") || upperValue.equals("N")) {
            return false;
        }
        try {
            return Boolean.parseBoolean(value);
        } catch (Exception e) {
            return false;
        }
    }

    public static String unescapeNewline(String value, boolean unescape) {
        if (unescape) {
            value = value.replace("\\n", "\n").replace("\\r", "\r");
        }
        return value;
    }

    /**
     * Checks if a string is in hexadecimal format.
     *
     * @param str the string to check
     * @return true if string is hexadecimal format
     */
    private static boolean isHexFormat(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return str.length() % 2 == 0;
    }

    /**
     * Imports CSV data into database with various control options.
     * <p>
     * This method provides a comprehensive interface for CSV data import with options configured via
     * {@link DBLoaderConfig} constants. All option names and string values are case-insensitive.
     * <p>
     * Options:
     * - DBLoaderConfig.SHOW: Controls what SQL statements to display and whether to execute them.
     * Default: "OFF"
     * Possible values: "TRUE", "1", "YES", "Y", "ON", "ALL", "DDL", "DML", "FALSE", "0", "NO", "N", "OFF"
     * <p>
     * - DBLoaderConfig.CREATE: Whether to generate DDL and create table before importing data.
     * Default: false
     * Possible values: boolean or string representations like "true", "1", "yes", "y", "on", "false", "0", "no", "n", "off"
     * <p>
     * - DBLoaderConfig.TRUNCATE: Whether to truncate target table before importing data.
     * Default: false
     * Possible values: boolean or string representations like "true", "1", "yes", "y", "on", "false", "0", "no", "n", "off"
     * <p>
     * - DBLoaderConfig.ERRORS: Maximum number of errors allowed before stopping import.
     * Default: -1 (unlimited)
     * Possible values: integer or string representation
     * <p>
     * - DBLoaderConfig.BATCH_ROWS: Number of rows to batch before committing.
     * Default: 2048
     * Possible values: integer or string representation
     * <p>
     * - DBLoaderConfig.PLATFORM: Database platform name.
     * Default: null (auto-detected from connection metadata)
     * Possible values: "oracle", "mysql", "mariadb", "db2", "mssql", "sqlserver", "pgsql", "postgresql"
     * <p>
     * - DBLoaderConfig.ROW_LIMIT: Maximum number of rows to process (0 for no limit).
     * Default: 0 (unlimited)
     * Possible values: integer or string representation
     * <p>
     * - DBLoaderConfig.REPORT_MB: Byte interval for progress logging (in MB).
     * Default: 10 MB
     * Set to -1 to disable all progress reporting.
     * Possible values: integer or string representation
     * <p>
     * - DBLoaderConfig.DELIMITER: Character to use for separating entries in CSV file.
     * Default: CSVParser.DEFAULT_SEPARATOR (',')
     * Possible values: character or string (first character used)
     * <p>
     * - DBLoaderConfig.ENCLOSURE: Character to use for quoted elements in CSV file.
     * Default: CSVParser.DEFAULT_QUOTE_CHARACTER ('"')
     * Possible values: character or string (first character used)
     * <p>
     * - DBLoaderConfig.ESCAPE: Character to use for escaping a separator or quote in CSV file.
     * Default: CSVParser.DEFAULT_ESCAPE_CHARACTER ('\\')
     * Possible values: character or string (first character used)
     * <p>
     * - DBLoaderConfig.SKIP_ROWS: Number of lines to skip at start of CSV file.
     * Default: CSVReader.DEFAULT_SKIP_LINES (0)
     * Note: If hasHeader is true, this is automatically set to 1
     * Possible values: integer or string representation
     * <p>
     * - DBLoaderConfig.ENCODING: Charset to use for reading the CSV file.
     * Default: null (auto-detection)
     * Use null, empty string, or "auto" for automatic charset detection.
     * Otherwise, uses the specified charset (e.g., "UTF-8", "GBK", "ISO-8859-1")
     * <p>
     * - DBLoaderConfig.VARIABLE_FORMAT: Variable placeholder format for SQL statements.
     * Default: '?' (JDBC standard)
     * Possible values: "?" or "jdbc" for JDBC standard, ":" or "oracle" for Oracle-style placeholders
     * <p>
     * Note: DELIMITER, ENCLOSURE, ESCAPE, SKIP_ROWS, ENCODING, and VARIABLE_FORMAT options are used for both DDL generation and data import.
     * <p>
     * Additional options from generateCreateTableDDL:
     * - DBLoaderConfig.SCAN_ROWS: Number of rows to scan for type detection during DDL generation.
     * Default: 200
     * Possible values: integer or string representation
     * <p>
     * - DBLoaderConfig.COLUMN_SIZE: Column size mode for DDL generation.
     * Default: "MAXIMUM"
     * Possible values: "ACTUAL" or "MAXIMUM"
     * <p>
     * - DBLoaderConfig.DATE_FORMAT: Date format string for parsing date values.
     * Default: null (auto-detection)
     * Use null or "auto" for automatic format detection.
     * <p>
     * - DBLoaderConfig.TIMESTAMP_FORMAT: Timestamp format string for parsing timestamp values.
     * Default: null (auto-detection)
     * Use null or "auto" for automatic format detection.
     * <p>
     * - DBLoaderConfig.TIMESTAMPTZ_FORMAT: Timestamp with timezone format string for parsing timestamp with timezone values.
     * Default: null (auto-detection)
     * Use null or "auto" for automatic format detection.
     * <p>
     * - DBLoaderConfig.MAP_COLUMN_NAMES: Map of CSV column names to database column names.
     * Default: null (no mapping)
     * Expected value: Map<String, String>
     * <p>
     * - DBLoaderConfig.UNESCAPE_NEWLINE: Whether to unescape newline characters in CSV data (\n to actual newline).
     * Default: true
     * Possible values: boolean or string representations
     * <p>
     * - DBLoaderConfig.SKIP_COLUMNS: Columns to skip during import.
     * Default: "auto" (automatic skipping of non-existent columns)
     * Possible values: "auto", "off", or a list of column names enclosed in parentheses like "(column1,column2)"
     * <p>
     * - DBLoaderConfig.COLUMN_INFO_SQL: SQL query to retrieve column information.
     * Default: null (uses Connection.getMetaData().getColumns() instead)
     * The query must return COLUMN_NAME, DATA_TYPE, TYPE_NAME, and COLUMN_SIZE columns
     *
     * @param connection  database connection
     * @param tableName   table name (null to use CSV filename without extension)
     * @param csvFilePath path to CSV file
     * @param options     configuration options map (can be null)
     * @return number of rows successfully imported
     * @throws Exception if an error occurs during import
     */
    public static long importCSVData(Connection connection, String tableName, String csvFilePath, Map<String, Object> options) throws Exception {

        if (csvFilePath == null || csvFilePath.trim().isEmpty()) {
            throw new Exception("CSV file path cannot be null or empty");
        }

        File csvFile = new File(csvFilePath);
        if (!csvFile.exists()) {
            throw new Exception("CSV file does not exist: " + csvFilePath);
        }

        // Parse options using DBLoaderConfig.parseOptions method
        DBLoaderConfig config = DBLoaderConfig.parseOptions(options);
        config.hasHeader = true;

        String databaseProductName = getDatabaseProductName(connection, config);
        config.log("Database: " + databaseProductName);

        buildCustomDateTimeFormatterCache(config);

        String showOption = config.show;
        boolean create = config.create;
        boolean truncate = config.truncate;
        int maxErrors = config.errors;
        int batchSize = config.batchSize;

        if (showOption != null) {
            String upperShowOption = showOption.toUpperCase().trim();
            if (upperShowOption.equals("DDL") || upperShowOption.equals("ALL")) {
                String ddl = generateCreateTableDDL(connection, tableName, csvFilePath, options);
                config.log(ddl);
            }

            if (upperShowOption.equals("DML") || upperShowOption.equals("ALL")) {
                String insertSql = generateInsertStatement(connection, tableName, csvFilePath, options);
                config.log(insertSql);
            }

            if (!upperShowOption.equals("OFF")) {
                return 0;
            }
        }

        if (connection == null || connection.isClosed()) {
            throw new Exception("Connection cannot be null or closed");
        }


        if (create) {
            String ddl = generateCreateTableDDL(connection, tableName, csvFilePath, options);
            config.log("Creating table with DDL:");
            config.log(ddl);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(ddl);
                config.log("Table created successfully");
            } catch (SQLException e) {
                config.log("Error creating table: " + e.getMessage());
                throw e;
            }
        }

        if (truncate) {
            String actualTableName = tableName;
            if (actualTableName == null || actualTableName.trim().isEmpty()) {
                actualTableName = csvFile.getName();
                int lastDotIndex = actualTableName.lastIndexOf('.');
                if (lastDotIndex > 0) {
                    actualTableName = actualTableName.substring(0, lastDotIndex);
                }
                if (actualTableName.isEmpty()) {
                    throw new Exception("Cannot determine table name from CSV file: " + csvFilePath);
                }
            }
            String truncateSql;
            String upperProductName = databaseProductName.toUpperCase();
            if (upperProductName.contains("ORACLE")) {
                truncateSql = "TRUNCATE TABLE " + actualTableName + " DROP STORAGE";
            } else {
                truncateSql = "TRUNCATE TABLE " + actualTableName;
            }

            config.log("Truncating table: " + truncateSql);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(truncateSql);
                config.log("Table truncated successfully");
            } catch (SQLException e) {
                config.log("Error truncating table: " + e.getMessage());
                throw e;
            }
        }

        config.log("Importing CSV data with batch size: " + batchSize);
        config.log("Max errors allowed: " + (maxErrors == -1 ? "unlimited" : maxErrors));

        // Create DBLoader using DBLoaderConfig
        DBLoader loader = new DBLoader(connection, tableName, csvFilePath, config);

        try {
            return loader.load();
        } catch (Exception e) {
            config.log("Error during import: " + e.getMessage());
            throw e;
        } finally {

        }
    }

    /**
     * Generates an INSERT statement based on CSV file analysis.
     * <p>
     * This method analyzes the CSV file structure and generates
     * an appropriate INSERT statement compatible with the target database.
     * All option names and string values are case-insensitive.
     *
     * @param connection  database connection used to determine database type
     * @param tableName   table name to use (null to use CSV filename without extension)
     * @param csvFilePath path to the CSV file
     * @param options     configuration options (can be null)
     *                    DBLoaderConfig.DELIMITER: character/String, default CSVParser.DEFAULT_SEPARATOR. Specifies column delimiter for CSV parsing.
     *                    String values: first character is used (case-insensitive)
     *                    DBLoaderConfig.ENCLOSURE: character/String, default CSVParser.DEFAULT_QUOTE_CHARACTER. Specifies enclosure character for CSV parsing.
     *                    String values: first character is used (case-insensitive)
     *                    DBLoaderConfig.ESCAPE: character/String, default CSVParser.DEFAULT_DBLoaderConfig.ESCAPE_CHARACTER. Specifies escape character for CSV parsing.
     *                    String values: first character is used (case-insensitive)
     *                    DBLoaderConfig.ENCODING: String, default auto-detection. Specifies charset for reading CSV file.
     *                    When set to empty string or "auto" (case-insensitive), uses automatic charset detection.
     *                    Otherwise, uses the specified charset (e.g., "UTF-8", "GBK", "ISO-8859-1") (case-insensitive)
     *                    DBLoaderConfig.VARIABLE_FORMAT: character/String, default "?". Specifies variable placeholder format for INSERT statement.
     *                    - "?" or "jdbc": uses JDBC standard placeholders "?" (default)
     *                    - ":" or "oracle": uses Oracle-style placeholders ":1", ":2", ":3", etc.
     *                    String values: first character is used (case-insensitive)
     * @return INSERT SQL statement
     * @throws Exception if file validation fails or header is missing
     */

    public static String generateInsertStatement(Connection connection, String tableName, String csvFilePath, Map<String, Object> options) throws Exception {
        return generateInsertStatementWithConfig(connection, tableName, csvFilePath, DBLoaderConfig.parseOptions(options));
    }

    public static String generateInsertStatementWithConfig(Connection connection, String tableName, String csvFilePath, DBLoaderConfig config) throws Exception {
        HashMap<String, Object[]> tableColumns = new HashMap<>();
        HashMap<String, String> columnMaps = new HashMap<>();
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement();
                 ResultSet metaData = config.columnInfoSQL != null && !config.columnInfoSQL.isEmpty()
                         ? stmt.executeQuery(config.columnInfoSQL)
                         : connection.getMetaData().getColumns(null, null, tableName, null)) {

                List<String> dbColumnList = new ArrayList<>();
                while (metaData.next()) {
                    String columnName = metaData.getString("COLUMN_NAME");
                    String columnTypeName = metaData.getString("TYPE_NAME");
                    int columnType = metaData.getInt("DATA_TYPE");
                    int columnSize = metaData.getInt("COLUMN_SIZE");
                    tableColumns.put(columnName.toUpperCase(), new Object[]{columnName, columnTypeName, columnType, columnSize});
                    dbColumnList.add(columnName);
                }
                config.tableColumns = dbColumnList.toArray(new String[0]);
            }
        }

        if (config.columnNameMap != null) {
            for (Map.Entry<String, String> entry : config.columnNameMap.entrySet()) {
                String dbColumn = entry.getValue();
                if (tableColumns.containsKey(dbColumn.toUpperCase())) {
                    dbColumn = (String) (tableColumns.get(dbColumn.toUpperCase())[0]);
                }
                columnMaps.put(entry.getKey().toUpperCase(), dbColumn);
            }
        }
        try (CSVReader csvReader = new CSVReader(csvFilePath,
                config.delimiter,
                config.quotechar,
                config.escape,
                config.skipLines,
                config.encoding)) {
            String[] headers = csvReader.readNext();
            if (headers == null || headers.length == 0) {
                throw new Exception("CSV file has no header row");
            }

            int validColumnCount = headers.length;
            if (!config.hasHeader) {
                if (config.tableColumns == null) {
                    throw new Exception("DBLoaderConfig.hasHeader is set to false, but no database columns found");
                }
                headers = config.tableColumns;
                validColumnCount = Math.min(headers.length, validColumnCount);
            } else {
                config.csvHeaders = headers;
            }

            if (tableName == null || tableName.isEmpty()) {
                File csvFile = new File(csvFilePath);
                String fileName = csvFile.getName();
                int lastDotIndex = fileName.lastIndexOf('.');
                if (lastDotIndex > 0) {
                    tableName = fileName.substring(0, lastDotIndex);
                } else {
                    tableName = fileName;
                }
            }

            String databaseProductName = getDatabaseProductName(connection, config);
            String[] columns = null;

            char variableFormatChar = config.variableFormat;

            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ").append(tableName).append(" (\n");

            ArrayList<String> columnList = new ArrayList<>();
            ArrayList<String> columnNames = new ArrayList<>();
            ArrayList<String> columnTypeNames = new ArrayList<>();
            ArrayList<Integer> columnTypes = new ArrayList<>();
            ArrayList<Integer> columnSizes = new ArrayList<>();
            ArrayList<Integer> csvColumns = new ArrayList<>();
            for (int i = 0; i < validColumnCount; i++) {
                String csvColumnName = headers[i].trim();
                // Use mapped column name if available, otherwise use original CSV column name
                String dbColumnName = columnMaps.containsKey(csvColumnName.toUpperCase())
                        ? columnMaps.get(csvColumnName.toUpperCase())
                        : csvColumnName;
                if (!config.skipColumns.containsKey(dbColumnName.toUpperCase())) {
                    Object[] columnInfo = tableColumns.get(dbColumnName.toUpperCase());
                    String dataType = null;
                    if (columnInfo != null) {
                        columnNames.add((String) columnInfo[0]);
                        dataType = (String) columnInfo[1];
                        columnTypeNames.add(dataType);
                        columnTypes.add((int) columnInfo[2]);
                        columnSizes.add((int) columnInfo[3]);
                    } else if (!tableColumns.isEmpty() && !config.skipColumns.containsKey(DBLoaderConfig.SKIP_COLUMNS_AUTO)) {
                        throw new Exception("Target column `" + dbColumnName + "` does not match any table column");
                    }
                    String column = quoteColumnName(dbColumnName, databaseProductName);
                    columnList.add(column);
                    sql.append("    ");
                    sql.append(columnList.size() > 1 ? "," : " ").append(column);
                    sql.append(dataType != null ? "  -- " + dataType : "").append("\n");
                    csvColumns.add(i);
                }
            }
            columns = columnList.toArray(new String[0]);
            config.tableColumns = columnNames.toArray(new String[0]);
            config.columnTypes = columnTypes.toArray(new Integer[0]);
            config.columnTypeNames = columnTypeNames.toArray(new String[0]);
            config.columnSizes = columnSizes.toArray(new Integer[0]);
            config.csvColumns = csvColumns.toArray(new Integer[0]);

            sql.append(") VALUES (\n");
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    sql.append(",\n");
                }
                if (variableFormatChar == ':') {
                    sql.append("    :").append(i + 1);
                } else {
                    sql.append("    ?");
                }
            }
            sql.append("\n)");
            return sql.toString();
        }
    }

    /**
     * Builds a custom DateTimeFormatter cache based on the provided DBLoaderConfig.
     * <p>
     * This method configures the custom DateTimeFormatter cache using the specified DBLoaderConfig.
     * It clears the existing cache and populates it with formatters for the provided date,
     * timestamp, and timestamptz formats. If no custom format is specified for any of these,
     * the default formatters are used.
     *
     * @param config the DBLoaderConfig containing date, timestamp, and timestamptz formats
     */
    public static void buildCustomDateTimeFormatterCache(DBLoaderConfig config) {
        String dateFormat = config.dateFormat;
        String timestampFormat = config.timestampFormat;
        String timestamptzFormat = config.timestamptzFormat;

        Locale locale = config.locale;

        config.dateTimeFormatters.clear();
        int counter = 0;
        if (dateFormat != null && !dateFormat.isEmpty() && !dateFormat.equalsIgnoreCase("auto")) {
            config.dateTimeFormatters.put(dateFormat, DateTimeFormatter.ofPattern(dateFormat, locale));
            counter++;
        }

        if (timestampFormat != null && !timestampFormat.isEmpty() && !timestampFormat.equalsIgnoreCase("auto")) {
            config.dateTimeFormatters.put(timestampFormat, DateTimeFormatter.ofPattern(timestampFormat, locale));
            counter++;
        }

        if (timestamptzFormat != null && !timestamptzFormat.isEmpty() && !timestamptzFormat.equalsIgnoreCase("auto")) {
            config.dateTimeFormatters.put(timestamptzFormat, DateTimeFormatter.ofPattern(timestamptzFormat, locale));
            counter++;
        }

        if (counter < 3) {
            DATETIME_FORMATTER_CACHE.forEach((k, v) -> {
                config.dateTimeFormatters.put(k, v.withLocale(locale));
            });
        }
    }

    /**
     * Generates a CREATE TABLE DDL statement based on CSV file analysis with custom table name.
     * <p>
     * This method analyzes the CSV file structure and data types to generate
     * an appropriate CREATE TABLE statement compatible with the target database.
     * All option names and string values are case-insensitive.
     *
     * @param connection  the database connection used to determine database type
     * @param tableName   the table name to use (null to use CSV filename without extension)
     * @param csvFilePath the path to the CSV file
     * @param options     configuration options (can be null)
     *                    DBLoaderConfig.SCAN_ROWS: integer/String, default 200. Number of rows to scan for type detection.
     *                    String values are parsed as integer (case-insensitive)
     *                    DBLoaderConfig.COLUMN_SIZE: String, default "MAXIMUM". Column size mode: "ACTUAL" or "MAXIMUM" (case-insensitive)
     *                    DBLoaderConfig.DATE_FORMAT: String, default auto-detection. Date format string for parsing dates.
     *                    When set to empty string or "auto" (case-insensitive), uses automatic format detection.
     *                    DBLoaderConfig.TIMESTAMP_FORMAT: String, default auto-detection. Timestamp format string for parsing timestamps.
     *                    When set to empty string or "auto" (case-insensitive), uses automatic format detection.
     *                    DBLoaderConfig.TIMESTAMPTZ_FORMAT: String, default auto-detection. Timestamp with timezone format string.
     *                    When set to empty string or "auto" (case-insensitive), uses automatic format detection.
     *                    DBLoaderConfig.MAP_COLUMN_NAMES: Map of CSV column names to database column names
     *                    DBLoaderConfig.DELIMITER: character/String, default CSVParser.DEFAULT_SEPARATOR. Specifies column delimiter for CSV parsing.
     *                    String values: first character is used (case-insensitive)
     *                    DBLoaderConfig.ENCLOSURE: character/String, default CSVParser.DEFAULT_QUOTE_CHARACTER. Specifies enclosure character for CSV parsing.
     *                    String values: first character is used (case-insensitive)
     *                    DBLoaderConfig.ESCAPE: character/String, default CSVParser.DEFAULT_DBLoaderConfig.ESCAPE_CHARACTER. Specifies escape character for CSV parsing.
     *                    String values: first character is used (case-insensitive)
     *                    DBLoaderConfig.SKIP_ROWS: integer/String, default CSVReader.DEFAULT_SKIP_LINES. Specifies number of lines to skip at start of CSV file.
     *                    String values are parsed as integer (case-insensitive)
     *                    DBLoaderConfig.ENCODING: String, default auto-detection. Specifies charset for reading CSV file.
     *                    When set to empty string or "auto" (case-insensitive), uses automatic charset detection.
     *                    Otherwise, uses the specified charset (e.g., "UTF-8", "GBK", "ISO-8859-1") (case-insensitive)
     * @return the CREATE TABLE DDL statement
     * @throws Exception if file validation fails or header is missing
     */
    public static String generateCreateTableDDL(Connection connection, String tableName, String csvFilePath, Map<String, Object> options) throws Exception {
        File csvFile = new File(csvFilePath);

        if (!csvFile.exists()) {
            throw new Exception("CSV file does not exist: " + csvFilePath);
        }

        if (!csvFile.canRead()) {
            throw new Exception("CSV file is not readable: " + csvFilePath);
        }

        // Parse options using DBLoaderConfig.parseOptions method
        DBLoaderConfig config = DBLoaderConfig.parseOptions(options);

        if (!config.hasHeader) {
            throw new Exception("CSV file has no header row and no csvHeaders is provided");
        }

        char delimiter = config.delimiter;
        char quotechar = config.quotechar;
        char escape = config.escape;
        int skips = config.skipLines;
        String encoding = config.encoding;

        try (CSVReader csvReader = new CSVReader(csvFilePath, delimiter, quotechar, escape, skips, encoding)) {
            String[] headers = csvReader.readNext();

            if (headers == null || headers.length == 0) {
                throw new Exception("CSV file has no header row");
            }

            config.csvHeaders = headers;

            if (tableName == null || tableName.isEmpty()) {
                String fileName = csvFile.getName();
                int lastDotIndex = fileName.lastIndexOf('.');
                if (lastDotIndex > 0) {
                    tableName = fileName.substring(0, lastDotIndex);
                } else {
                    tableName = fileName;
                }
            }

            int actualSampleRows = config.scanRows;

            int columnCount = headers.length;
            List<List<String>> columnValues = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                columnValues.add(new ArrayList<>());
            }

            int rowsRead = 0;
            String[] row;
            while ((row = csvReader.readNext()) != null && rowsRead < actualSampleRows) {
                for (int i = 0; i < Math.min(row.length, columnCount); i++) {
                    if (row[i] != null && !row[i].trim().isEmpty()) {
                        columnValues.get(i).add(row[i].trim());
                    }
                }
                rowsRead++;
            }

            String databaseProductName = getDatabaseProductName(connection, config);

            String columnSizeMode = config.columnSize;
            Map<String, String> columnNameMap = config.columnNameMap;

            buildCustomDateTimeFormatterCache(config);

            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE TABLE ").append(tableName).append(" (\n");

            for (int i = 0; i < columnCount; i++) {
                if (i > 0) {
                    ddl.append(",\n");
                }

                String csvColumnName = headers[i].trim();
                String columnName = csvColumnName;
                if (columnName.isEmpty()) {
                    columnName = "COLUMN_" + (i + 1);
                } else if (columnNameMap != null && columnNameMap.containsKey(csvColumnName)) {
                    String mappedName = columnNameMap.get(csvColumnName);
                    if (mappedName != null && !mappedName.isEmpty()) {
                        columnName = mappedName;
                    }
                }

                List<String> values = columnValues.get(i);
                int sqlType = detectColumnType(values, config.unescapeNewline, config);

                String dbType = getDatabaseTypeName(sqlType, databaseProductName, values, columnSizeMode);
                ddl.append("    ").append(columnName).append(" ").append(dbType);
            }

            ddl.append("\n)");
            return ddl.toString();
        }
    }

    /**
     * Gets database product name from DBLoaderConfig or connection metadata.
     *
     * @param connection database connection
     * @param config     DBLoaderConfig object containing platform information
     * @return database product name
     */
    private static String getDatabaseProductName(Connection connection, DBLoaderConfig config) {
        String platform = config.platform;

        if (platform != null && !platform.isEmpty()) {
            String upperPlatform = platform.trim().toUpperCase();
            if (!upperPlatform.equals("AUTO")) {
                if (upperPlatform.equals("ORACLE")) {
                    return "Oracle";
                } else if (upperPlatform.equals("MYSQL") || upperPlatform.equals("MARIADB")) {
                    return "MySQL";
                } else if (upperPlatform.equals("DB2")) {
                    return "DB2";
                } else if (upperPlatform.equals("MSSQL") || upperPlatform.equals("SQLSERVER")) {
                    return "Microsoft SQL Server";
                } else if (upperPlatform.equals("PGSQL") || upperPlatform.equals("POSTGRESQL")) {
                    return "PostgreSQL";
                } else {
                    config.log("Warning: Invalid DBLoaderConfig.PLATFORM value '" + platform + "', using connection metadata");
                }
            }
        }
        try {
            platform = connection.getMetaData().getDatabaseProductName().trim().toUpperCase();
            if (platform.equals("ORACLE")) {
                platform = "Oracle";
            } else if (platform.equals("MYSQL") || platform.equals("MARIADB")) {
                platform = "MySQL";
            } else if (platform.equals("DB2")) {
                platform = "DB2";
            } else if (platform.equals("MSSQL") || platform.equals("SQLSERVER")) {
                platform = "Microsoft SQL Server";
            } else if (platform.equals("PGSQL") || platform.equals("POSTGRESQL")) {
                platform = "PostgreSQL";
            } else {
                config.log("Warning: Invalid DBLoaderConfig.PLATFORM value '" + platform + "', using connection metadata");
            }
            config.platform = platform;
            return platform;
        } catch (SQLException e) {
            config.log("Warning: Could not get database product name, defaulting to Oracle");
            config.platform = "Oracle";
            return "Oracle";
        }
    }

    /**
     * Detects the SQL data type for a column based on sample values.
     *
     * @param values list of sample values for the column
     * @return the SQL type constant from java.sql.Types
     */
    private static int detectColumnType(List<String> values, boolean unescape, DBLoaderConfig config) {
        if (values == null || values.isEmpty()) {
            return Types.VARCHAR;
        }

        int booleanCount = 0;
        int integerCount = 0;
        int bigIntCount = 0;
        int decimalCount = 0;
        int dateCount = 0;
        int timestampCount = 0;
        int timestampTzCount = 0;
        int timeCount = 0;
        int binaryCount = 0;
        int totalCount = 0;

        for (String value : values) {
            if (value == null || value.isEmpty()) {
                continue;
            }

            totalCount++;
            Integer dateType = isDateTimeValue(value, config);
            Number num;
            if (dateType != null) {
                if (dateType == Types.DATE) {
                    dateCount++;
                } else if (dateType == Types.TIMESTAMP) {
                    timestampCount++;
                } else if (dateType == Types.TIMESTAMP_WITH_TIMEZONE) {
                    timestampTzCount++;
                } else if (dateType == Types.TIME) {
                    timeCount++;
                }
            } else if (isBooleanValue(value) && (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))) {
                booleanCount++;
            } else if (isTimeValue(value)) {
                timeCount++;
            } else if ((num = parseNumeric(value)) != null) {
                if (num instanceof Long || num instanceof BigInteger) {
                    bigIntCount++;
                } else if (num instanceof BigDecimal || num instanceof Float || num instanceof Double) {
                    decimalCount++;
                } else {
                    integerCount++;
                }
            } else if (isBinaryValue(unescapeNewline(value, unescape))) {
                binaryCount++;
            }
        }

        if (totalCount == 0) {
            return Types.VARCHAR;
        }

        double threshold = totalCount * 0.8;

        if (binaryCount >= threshold) {
            return Types.VARBINARY;
        }

        if (timestampTzCount >= threshold) {
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }

        if (timestampCount >= threshold) {
            return Types.TIMESTAMP;
        }

        if (timeCount >= threshold) {
            return Types.TIME;
        }

        if (dateCount >= threshold) {
            return Types.DATE;
        }

        if (booleanCount >= threshold) {
            return Types.BOOLEAN;
        }

        if (decimalCount > 0) {
            return Types.DECIMAL;
        }

        if (bigIntCount >= threshold) {
            return Types.BIGINT;
        }

        if (integerCount >= threshold) {
            return Types.INTEGER;
        }

        return Types.VARCHAR;
    }

    /**
     * Gets the database-specific type name for a SQL type.
     *
     * @param sqlType             the SQL type constant from java.sql.Types
     * @param databaseProductName the database product name
     * @param values              sample values for determining precision/scale
     * @param columnSizeMode      "ACTUAL" or "MAXIMUM" mode for column size
     * @return the database-specific type name
     */
    private static String getDatabaseTypeName(int sqlType, String databaseProductName, List<String> values, String columnSizeMode) {
        String upperProductName = databaseProductName.toUpperCase();

        switch (sqlType) {
            case Types.BOOLEAN:
                if (upperProductName.contains("ORACLE")) {
                    return "NUMBER(1)";
                }
                return "BOOLEAN";

            case Types.INTEGER:
                if (upperProductName.contains("ORACLE")) {
                    return "NUMBER(10)";
                }
                return "INTEGER";

            case Types.BIGINT:
                if (upperProductName.contains("ORACLE")) {
                    return "NUMBER(19)";
                }
                return "BIGINT";

            case Types.DECIMAL:
                if (DBLoaderConfig.COLUMN_SIZE_ACTUAL.equals(columnSizeMode)) {
                    int[] decimalParams = calculateDecimalParams(values);
                    if (upperProductName.contains("POSTGRESQL")) {
                        return "NUMERIC(" + decimalParams[0] + "," + decimalParams[1] + ")";
                    }
                    return "DECIMAL(" + decimalParams[0] + "," + decimalParams[1] + ")";
                }
                if (upperProductName.contains("POSTGRESQL")) {
                    return "NUMERIC(38,10)";
                }
                return "DECIMAL(38,10)";

            case Types.DATE:
                return "DATE";

            case Types.TIME:
                if (upperProductName.contains("ORACLE")) {
                    return "TIMESTAMP";
                }
                return "TIME";

            case Types.TIMESTAMP:
                if (upperProductName.contains("MYSQL")) {
                    return "DATETIME";
                }
                return "TIMESTAMP";

            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (upperProductName.contains("MYSQL")) {
                    return "DATETIME";
                } else if (upperProductName.contains("ORACLE")) {
                    return "TIMESTAMP WITH TIME ZONE";
                }
                return "TIMESTAMPTZ";

            case Types.VARBINARY:
                int binarySize = DBLoaderConfig.COLUMN_SIZE_ACTUAL.equals(columnSizeMode) ? calculateMaxLength(values, 4000, 32767) : 4000;
                if (upperProductName.contains("ORACLE")) {
                    return "RAW(" + binarySize + ")";
                }
                return "VARBINARY(" + binarySize + ")";

            case Types.VARCHAR:
            default:
                int varcharSize = DBLoaderConfig.COLUMN_SIZE_ACTUAL.equals(columnSizeMode) ? calculateMaxLength(values, 255, 4000) : 255;
                if (upperProductName.contains("ORACLE")) {
                    return "VARCHAR2(" + varcharSize + ")";
                } else if (upperProductName.contains("SQL SERVER")) {
                    return "NVARCHAR(" + varcharSize + ")";
                }
                return "VARCHAR(" + varcharSize + ")";
        }
    }

    /**
     * Calculates decimal precision and scale from sample values.
     *
     * @param values sample values
     * @return int array with [precision, scale]
     */
    private static int[] calculateDecimalParams(List<String> values) {
        int precision = 38;
        int scale = 10;
        for (String value : values) {
            if (value != null && !value.isEmpty() && parseNumeric(value) != null) {
                String[] parts = value.split("\\.");
                if (parts.length > 1) {
                    int currentScale = parts[1].length();
                    if (currentScale > scale) {
                        scale = Math.min(currentScale, 38);
                    }
                }
                int currentPrecision = value.replace(".", "").replace("-", "").length();
                if (currentPrecision > precision) {
                    precision = Math.min(currentPrecision, 38);
                }
            }
        }
        return new int[]{precision, scale};
    }

    /**
     * Calculates maximum length from sample values.
     *
     * @param values      sample values
     * @param defaultSize default size
     * @param maxSize     maximum allowed size
     * @return calculated length
     */
    private static int calculateMaxLength(List<String> values, int defaultSize, int maxSize) {
        int maxLength = defaultSize;
        for (String value : values) {
            if (value != null && value.length() > maxLength) {
                maxLength = Math.min(value.length(), maxSize);
            }
        }
        return maxLength;
    }

    /**
     * Checks if a string value represents a boolean.
     *
     * @param value the string to check
     * @return true if the value is a boolean representation
     */
    private static boolean isBooleanValue(String value) {
        String upperValue = value.toUpperCase().trim();
        return upperValue.equals("TRUE") || upperValue.equals("FALSE") ||
                upperValue.equals("1") || upperValue.equals("0") ||
                upperValue.equals("YES") || upperValue.equals("NO") ||
                upperValue.equals("Y") || upperValue.equals("N");
    }

    /**
     * Checks if a string value represents a binary value.
     *
     * @param value the string to check
     * @return true if the value is a binary representation
     */
    private static boolean isBinaryValue(String value) {
        String trimmed = value.trim();
        if (trimmed.isEmpty() || trimmed.indexOf(" ") > -1) return false;
        if (trimmed.length() % 2 != 0) return false;
        if ((trimmed.startsWith("0x") || trimmed.startsWith("0X")) && trimmed.indexOf(" ") == -1) {
            String hex = trimmed.substring(2);
            return isHexFormat(hex);
        }
        return isHexFormat(trimmed);
    }

    /**
     * Checks if a string value represents a date or timestamp.
     * Uses cache for format detection.
     *
     * @param value the string to check
     * @return true if the value is a date/timestamp representation
     */
    private static Integer isDateTimeValue(String value, DBLoaderConfig config) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        for (DateTimeFormatter formatter : config.dateTimeFormatters.values()) {
            try {
                TemporalAccessor temporal = formatter.parse(value);
                if (temporal instanceof LocalDate) {
                    return Types.DATE;
                } else if (temporal.query(TemporalQueries.zone()) != null || temporal.query(TemporalQueries.offset()) != null) {
                    return Types.TIMESTAMP_WITH_TIMEZONE;
                } else if (temporal.isSupported(ChronoField.NANO_OF_SECOND)
                        && temporal.get(ChronoField.NANO_OF_SECOND) != 0) {
                    return Types.TIMESTAMP;
                } else {
                    return Types.DATE;
                }
            } catch (DateTimeParseException e) {
            }
        }
        return null;
    }

    /**
     * Checks if a string value represents a time.
     *
     * @param value the string to check
     * @return true if the value is a time representation
     */
    private static boolean isTimeValue(String value) {
        for (DateTimeFormatter formatter : TIME_FORMATTER_CACHE.values()) {
            try {
                TemporalAccessor temporal = formatter.parse(value);
                if (temporal instanceof LocalTime || temporal instanceof OffsetTime) {
                    return true;
                }
            } catch (DateTimeParseException e) {
            }
        }
        return false;
    }

    /**
     * Loads data from CSV file into the database table.
     * <p>
     * This method performs the following steps:
     * 1. Reads table metadata (column names and types)
     * 2. Maps CSV headers to database columns (if hasHeader is true)
     * 3. Filters to only include columns that exist in CSV
     * 4. Creates PreparedStatement with appropriate INSERT statement
     * 5. Initializes .bad file for failed rows
     * 6. Processes each CSV row, converting data types and adding to batch
     * 7. Executes batch commits for efficiency
     * 8. Logs progress at regular intervals
     *
     * @return the number of rows successfully processed
     * @throws Exception if an error occurs during loading
     */
    public long load() throws Exception {
        startTime = System.currentTimeMillis();
        lastProgressTime = startTime;
        totalRowsProcessed = 0;
        totalErrors = 0;
        totalBytesProcessed = 0;
        lastProgressRows = 0;

        long progressIntervalMB = config.progressIntervalBytes / (1024 * 1024);
        config.log("Starting CSV load from " + csvFilePath + " to table " + tableName);
        config.log("Has header: " + config.hasHeader);
        config.log("Batch size: " + config.batchSize);
        config.log("Progress interval: " + (progressIntervalMB > 0 ? progressIntervalMB : "default") + " MB");

        // Create PreparedStatement
        if (connection == null || connection.isClosed()) {
            throw new SQLException("Connection is null or closed");
        }
        String insertSQL = generateInsertStatementWithConfig(connection, tableName, csvFilePath, config);

        // Initialize .bad file
        badFilePath = csvFilePath + ".bad";
        File badFile = new File(badFilePath);
        if (badFile.exists()) {
            badFile.delete();
        }
        int batchCount = 0;
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        if (config.dateTimeFormatters.isEmpty()) {
            DATETIME_FORMATTER_CACHE.forEach((k, v) -> {
                config.dateTimeFormatters.put(k, v.withLocale(config.locale));
            });
        }
        if (config.timeFormatters.isEmpty()) {
            TIME_FORMATTER_CACHE.forEach((k, v) -> {
                config.timeFormatters.put(k, v.withLocale(config.locale));
            });
        }
        isNeedRebuildTimestampFormat = config.dateTimeFormatters.size() > 5;
        isNeedRebuildTimeFormat = config.timeFormatters.size() > 5;
        timestampFormatCount = 0;
        timeFormatCount = 0;

        SQLException firstException = null;
        try (CSVReader csvReader = new CSVReader(
                csvFilePath,
                config.delimiter,
                config.quotechar,
                config.escape,
                config.skipLines + (config.hasHeader ? 1 : 0),
                config.encoding);
             PreparedStatement pstmt = connection.prepareStatement(insertSQL);
             CSVWriter badWriter = new CSVWriter(badFilePath)) {

            badFileWriter = badWriter; // Store reference for writeBadRow method
            int rowIndex = 0;
            long startProgressTime = System.currentTimeMillis();
            if (config.hasHeader && config.csvHeaders != null) {
                badWriter.writeNext(config.csvHeaders);
            }
            String[][] batchRows = new String[config.batchSize][];
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                ++rowIndex;
                if (config.rowLimit > 0 && rowIndex + 1 >= config.rowLimit) {
                    break;
                }
                totalBytesProcessed += calculateRowSize(row);
                totalRowsProcessed++;
                try {
                    // Add row to batch
                    int paramIndex = 1;
                    for (int i = 0; i < config.csvColumns.length; i++) {
                        String value = row.length > config.csvColumns[i] ? row[config.csvColumns[i]] : "";
                        setPreparedStatementParameter(pstmt, paramIndex, config.columnTypes[i], value);
                        paramIndex++;
                    }
                    pstmt.addBatch();
                    batchRows[batchCount] = row;
                    batchCount++;
                    // Rebuild time/timestamp formatters if needed
                    if (totalRowsProcessed >= 30) {
                        if (isNeedRebuildTimestampFormat && timestampFormatCount >= 100) {
                            config.dateTimeFormatters.clear();
                            config.dateTimeFormatters.putAll(config.runtimeDateTimeFormatterCache);
                            isNeedRebuildTimestampFormat = false;
                            config.runtimeDateTimeFormatterCache.clear();
                            timestampFormatCount = 0;
                        }

                        if (isNeedRebuildTimeFormat && timeFormatCount >= 100) {
                            config.timeFormatters.clear();
                            config.timeFormatters.putAll(config.runtimeTimeFormatterCache);
                            isNeedRebuildTimeFormat = false;
                            config.runtimeTimeFormatterCache.clear();
                            timeFormatCount = 0;
                        }
                    }

                    if (batchCount >= config.batchSize) {
                        try {
                            pstmt.executeBatch();
                        } catch (BatchUpdateException e) {
                            handleBatchUpdateException(e, batchRows, batchCount);
                        }

                        connection.commit();
                        pstmt.clearBatch();
                        batchCount = 0;
                        logProgress();
                        Arrays.fill(batchRows, null);
                    }
                } catch (SQLException e) {
                    handleError(e.getMessage());
                    writeBadRow(row, e.getMessage());
                    config.log("Error processing row " + (rowIndex + config.skipLines) + ": " + e.getMessage());
                }
            }

            // Execute remaining batch
            if (batchCount > 0) {
                try {
                    pstmt.executeBatch();
                } catch (BatchUpdateException e) {
                    handleBatchUpdateException(e, batchRows, batchCount);
                    pstmt.clearBatch();
                }
            }
            connection.commit();

            logProgress();
            String message = String.format("[%s] Load completed in %.2f seconds. Total rows processed: %d, Total successful: %d, Total errors: %d, MB processed: %.2f, successful/sec: %.2f",
                    java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    (System.currentTimeMillis() - startProgressTime) / 1000.0,
                    totalRowsProcessed,
                    totalRowsProcessed - totalErrors,
                    totalErrors,
                    totalBytesProcessed / (1024.0 * 1024.0),
                    ((totalRowsProcessed - totalErrors) * 1000.0) / (System.currentTimeMillis() - startProgressTime));
            config.log(message);
            return totalRowsProcessed;
        } catch (Exception e) {
            firstException = e instanceof SQLException ? (SQLException) e : new SQLException(e.getMessage(), e);
            throw e;
        } finally {
            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                if (firstException == null) {
                    firstException = e;
                }
                config.log("Error restoring auto-commit: " + e.getMessage());
            }

            // Clear the badFileWriter reference
            badFileWriter = null;

            if (firstException != null) {
                throw firstException;
            }
        }
    }

    /**
     * Handles BatchUpdateException by identifying failed rows and writing them to .bad file.
     *
     * @param e         BatchUpdateException
     * @param batchRows current rows being processed
     * @param batchSize batch size
     */
    private void handleBatchUpdateException(BatchUpdateException e, String[][] batchRows, int batchSize) {
        final int[] updateCounts = e.getUpdateCounts();
        String error = e.getMessage().trim();
        writeBadRow(null, error);
        int failedRows = 0;
        final int doneRows = updateCounts.length;
        for (int i = 0; i < batchSize; i++) {
            if (i >= doneRows || updateCounts[i] == Statement.EXECUTE_FAILED) {
                handleError("Batch update failed at position " + (i + 1));
                writeBadRow(batchRows[i], null);
                failedRows++;
            }
        }
        final int pos = error.indexOf("\n");
        error = error.substring(0, pos > 0 ? pos : error.length());
        config.log("Total failed rows: " + failedRows + ", failed with " + (1 + updateCounts.length) + "th row: " + error);
    }

    /**
     * Sets a PreparedStatement parameter with appropriate type conversion.
     * Similar to convertAndSetValue but uses PreparedStatement methods.
     *
     * @param pstmt      PreparedStatement to set parameter on
     * @param paramIndex 1-based parameter index
     * @param sqlType    SQL type from Types class
     * @param value      CSV string value
     * @throws SQLException if conversion fails or value is invalid
     */
    private void setPreparedStatementParameter(PreparedStatement pstmt, int paramIndex, int sqlType, String value) throws SQLException {
        if (value == null || value.isEmpty()) {
            pstmt.setNull(paramIndex, sqlType);
            return;
        }

        String trimmedValue = null;
        boolean needsTrim = (sqlType == Types.VARCHAR || sqlType == Types.CHAR ||
                sqlType == Types.NVARCHAR || sqlType == Types.NCHAR ||
                sqlType == Types.LONGVARCHAR || sqlType == Types.LONGNVARCHAR);

        if (needsTrim) {
            trimmedValue = value.trim();
            if (trimmedValue.isEmpty()) {
                pstmt.setNull(paramIndex, sqlType);
                return;
            }
            value = trimmedValue;
        } else {
            if (value.trim().isEmpty()) {
                pstmt.setNull(paramIndex, sqlType);
                return;
            }
        }
        Number num;
        try {
            switch (sqlType) {
                case Types.BIGINT:
                    num = parseNumeric(value);
                    if (num instanceof BigInteger) {
                        pstmt.setBigDecimal(paramIndex, new BigDecimal((BigInteger) num));
                    } else if (num != null && !(num instanceof Double || num instanceof Float || num instanceof BigDecimal)) {
                        pstmt.setLong(paramIndex, num instanceof Long ? (Long) num : num.longValue());
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;

                case Types.INTEGER:
                    num = parseNumeric(value);
                    if (num instanceof Integer || num instanceof Short || num instanceof Byte) {
                        pstmt.setInt(paramIndex, num instanceof Integer ? (Integer) num : num.intValue());
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;

                case Types.SMALLINT:
                    num = parseNumeric(value);
                    if (num instanceof Short || num instanceof Byte) {
                        pstmt.setShort(paramIndex, num instanceof Short ? (Short) num : num.shortValue());
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;

                case Types.TINYINT:
                    num = parseNumeric(value);
                    if (num instanceof Byte) {
                        pstmt.setByte(paramIndex, (Byte)num);
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                case 96: //BIGINT_UNSIGNED   
                    num = parseNumeric(value);
                    if (num != null) {
                        pstmt.setBigDecimal(paramIndex, num instanceof BigDecimal ? (BigDecimal) num : new BigDecimal(value));
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;
                case Types.DOUBLE:
                    num = parseNumeric(value);
                    if (num instanceof BigInteger) {
                        double doubleValue = ((BigInteger) num).doubleValue();
                        if (((BigInteger) num).compareTo(BigInteger.valueOf((long) doubleValue)) == 0) {
                            pstmt.setDouble(paramIndex, doubleValue);
                        } else {
                            throw new SQLException("Invalid numeric value: " + value);
                        }
                    } else if (num != null && !(num instanceof BigDecimal)) {
                        pstmt.setDouble(paramIndex, num instanceof Double ? (Double) num : num.doubleValue());
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    num = parseNumeric(value);
                    if (num instanceof BigInteger) {
                        float floatValue = ((BigInteger) num).floatValue();
                        if (((BigInteger) num).compareTo(BigInteger.valueOf((long) floatValue)) == 0) {
                            pstmt.setFloat(paramIndex, floatValue);
                        } else {
                            throw new SQLException("Invalid numeric value: " + value);
                        }
                    } else if (num != null && !(num instanceof BigDecimal)) {
                        pstmt.setFloat(paramIndex, num instanceof Float ? (Float) num : num.floatValue());
                    } else {
                        throw new SQLException("Invalid numeric value: " + value);
                    }
                    break;
                case Types.DATE:
                    Date date = parseDate(value);
                    if (date != null) {
                        pstmt.setDate(paramIndex, new java.sql.Date(date.getTime()));
                    } else {
                        throw new SQLException("Invalid date format: " + value);
                    }
                    break;
                case Types.TIME:
                    java.sql.Time time = parseTime(value);
                    if (time != null) {
                        pstmt.setTime(paramIndex, time);
                    } else {
                        throw new SQLException("Unable to parse time value: " + value);
                    }
                    break;
                case Types.TIME_WITH_TIMEZONE:
                    OffsetTime offsetTime = parseTimeWithTimezone(value);
                    if (offsetTime != null) {
                        pstmt.setObject(paramIndex, offsetTime);
                    } else {
                        throw new SQLException("Unable to parse time with timezone value: " + value);
                    }
                    break;

                case Types.TIMESTAMP:
                case -100: // Oracle TIMESTAMPNS
                    Timestamp timestamp = parseTimestamp(value);
                    if (timestamp != null) {
                        pstmt.setTimestamp(paramIndex, timestamp);
                    } else {
                        throw new SQLException("Unable to parse timestamp value: " + value);
                    }
                    break;

                case Types.TIMESTAMP_WITH_TIMEZONE:
                case -101: // Oracle TIMESTAMPTZ
                case -102: // Oracle TIMESTAMPLTZ
                    OffsetDateTime offsetDateTime = parseTimestampWithTimezone(value);
                    if (offsetDateTime != null) {
                        pstmt.setObject(paramIndex, offsetDateTime);
                    } else {
                        throw new SQLException("Unable to parse timestamp with timezone value: " + value);
                    }
                    break;

                case Types.BOOLEAN:
                case Types.BIT:
                case 252: // Oracle BOOLEAN
                    boolean boolValue = parseBoolean(value);
                    pstmt.setBoolean(paramIndex, boolValue);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    value = unescapeNewline(value, config.unescapeNewline);
                    byte[] bytes = parseBinary(value);
                    if (bytes != null) {
                        pstmt.setBytes(paramIndex, bytes);
                    } else {
                        throw new SQLException("Invalid binary data: " + value);
                    }
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.SQLXML:
                case 2016: // Oracle JSON
                case -105: // Oracle VECTOR
                case -106: // Oracle VECTOR_INT8
                case -107: // Oracle VECTOR_FLOAT32
                case -108: // Oracle VECTOR_FLOAT64
                case -109: // Oracle VECTOR_BINARY
                    value = unescapeNewline(value, config.unescapeNewline);
                    pstmt.setString(paramIndex, value);
                    break;

                default:
                    pstmt.setString(paramIndex, value);
                    break;
            }
        } catch (NumberFormatException e) {
            throw new SQLException("Number format error: " + e.getMessage());
        }
    }

    private void appendTimestampFormatters(String key, DateTimeFormatter formatter) {
        if (isNeedRebuildTimestampFormat) {
            if (!config.runtimeDateTimeFormatterCache.containsKey(key)) {
                config.runtimeDateTimeFormatterCache.put(key, formatter);
            }
            timestampFormatCount++;
        }
    }

    private void appendTimeFormatters(String key, DateTimeFormatter formatter) {
        if (isNeedRebuildTimeFormat) {
            if (!config.runtimeTimeFormatterCache.containsKey(key)) {
                config.runtimeTimeFormatterCache.put(key, formatter);
            }
            timeFormatCount++;
        }
    }

    /**
     * Parses a date-time string using multiple supported formats and returns an Instant.
     * Tries each format in order until one successfully parses the value.
     *
     * @param value the date-time string to parse
     * @return the parsed Instant, or null if parsing fails
     */
    private Instant parseToInstant(String value) {
        for (String key : config.dateTimeFormatters.keySet()) {
            try {
                DateTimeFormatter formatter = config.dateTimeFormatters.get(key);
                TemporalAccessor temporal = formatter.parse(value);
                appendTimestampFormatters(key, formatter);
                Instant instant = null;

                if (temporal instanceof Instant) {
                    instant = (Instant) temporal;
                } else if (temporal instanceof ZonedDateTime) {
                    final ZonedDateTime zdt = (ZonedDateTime) temporal;
                    instant = LocalDateTime.from(zdt).atZone(java.time.ZoneId.systemDefault()).toInstant();
                } else if (temporal instanceof OffsetDateTime) {
                    final OffsetDateTime odt = (OffsetDateTime) temporal;
                    instant = odt.toInstant();
                } else if (temporal instanceof LocalDateTime) {
                    final LocalDateTime ldt = (LocalDateTime) temporal;
                    instant = ldt.atZone(java.time.ZoneId.systemDefault()).toInstant();
                } else if (temporal instanceof LocalDate) {
                    final LocalDate ld = (LocalDate) temporal;
                    instant = ld.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant();
                } else {
                    // Try to create Instant from other TemporalAccessor types
                    try {
                        instant = Instant.from(temporal);
                    } catch (DateTimeException e) {
                        try {
                            // Try LocalDateTime first
                            LocalDateTime ldt = LocalDateTime.from(temporal);
                            instant = ldt.atZone(java.time.ZoneId.systemDefault()).toInstant();
                        } catch (DateTimeException e1) {
                            // Try LocalDate as fallback
                            try {
                                LocalDate ld = LocalDate.from(temporal);
                                instant = ld.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant();
                            } catch (DateTimeException e2) {
                                // Skip to next formatter
                                continue;
                            }
                        }
                    }
                }

                return instant;
            } catch (DateTimeParseException e) {
                // Skip to next formatter
            }
        }
        return null;
    }

    /**
     * Parses a date-time string using multiple supported formats and returns an OffsetDateTime.
     * Tries each format in order until one successfully parses the value.
     *
     * @param value the date-time string to parse
     * @return the parsed OffsetDateTime, or null if parsing fails
     */
    private OffsetDateTime parseToOffsetDateTime(String value) {
        for (String key : config.dateTimeFormatters.keySet()) {
            try {
                DateTimeFormatter formatter = config.dateTimeFormatters.get(key);
                TemporalAccessor temporal = formatter.parse(value);
                appendTimestampFormatters(key, formatter);

                if (temporal instanceof Instant) {
                    Instant instant = (Instant) temporal;
                    return OffsetDateTime.ofInstant(instant, java.time.ZoneId.systemDefault().getRules().getOffset(instant));
                } else if (temporal instanceof ZonedDateTime) {
                    ZonedDateTime zdt = (ZonedDateTime) temporal;
                    return zdt.toOffsetDateTime();
                } else if (temporal instanceof OffsetDateTime) {
                    return (OffsetDateTime) temporal;
                } else if (temporal instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) temporal;
                    return ldt.atZone(java.time.ZoneId.systemDefault()).toOffsetDateTime();
                } else if (temporal instanceof LocalDate) {
                    LocalDate ld = (LocalDate) temporal;
                    return ld.atStartOfDay(java.time.ZoneId.systemDefault()).toOffsetDateTime();
                } else {
                    // Try to create LocalDateTime first
                    try {
                        LocalDateTime ldt = LocalDateTime.from(temporal);
                        return ldt.atZone(java.time.ZoneId.systemDefault()).toOffsetDateTime();
                    } catch (DateTimeException e1) {
                        // Try LocalDate as fallback
                        try {
                            LocalDate ld = LocalDate.from(temporal);
                            return ld.atStartOfDay(java.time.ZoneId.systemDefault()).toOffsetDateTime();
                        } catch (DateTimeException e2) {
                            // Try Instant as last resort
                            try {
                                Instant instant = Instant.from(temporal);
                                return OffsetDateTime.ofInstant(instant, java.time.ZoneId.systemDefault().getRules().getOffset(instant));
                            } catch (DateTimeException e3) {
                                // Skip to next formatter
                                continue;
                            }
                        }
                    }
                }
            } catch (DateTimeParseException e) {
                // Skip to next formatter
            }
        }
        return null;
    }

    /**
     * Parses a date string using multiple supported formats.
     * Tries each format in order until one successfully parses the value.
     *
     * @param value the date string to parse
     * @return the parsed Date, or null if parsing fails
     */
    private Date parseDate(String value) {
        Instant instant = parseToInstant(value);
        return instant != null ? Date.from(instant) : null;
    }

    private Timestamp parseTimestamp(String value) {
        Instant instant = parseToInstant(value);
        return instant != null ? Timestamp.from(instant) : null;
    }

    private OffsetDateTime parseTimestampWithTimezone(String value) {
        return parseToOffsetDateTime(value);
    }

    private OffsetTime parseTimeWithTimezone(String value) {
        for (String key : config.timeFormatters.keySet()) {
            try {
                DateTimeFormatter formatter = config.timeFormatters.get(key);
                TemporalAccessor temporal = formatter.parse(value);
                appendTimeFormatters(key, formatter);
                if (temporal instanceof OffsetTime) {
                    return (OffsetTime) temporal;
                } else if (temporal instanceof LocalTime) {
                    LocalTime lt = (LocalTime) temporal;
                    return lt.atOffset(java.time.ZoneOffset.UTC);
                } else {
                    throw new DateTimeParseException("Unsupported time type", value, 0);
                }
            } catch (DateTimeParseException e) {
            }
        }
        return null;
    }

    /**
     * Parses a time string using multiple supported formats.
     * Supports various time formats including timezone-aware formats.
     *
     * @param value the time string to parse
     * @return the parsed Date, or null if parsing fails
     */
    private java.sql.Time parseTime(String value) {
        for (String key : config.timeFormatters.keySet()) {
            try {
                DateTimeFormatter formatter = config.timeFormatters.get(key);
                TemporalAccessor temporal = formatter.parse(value);
                appendTimeFormatters(key, formatter);
                if (temporal instanceof OffsetTime) {
                    OffsetTime ot = (OffsetTime) temporal;
                    return java.sql.Time.valueOf(LocalTime.from(ot));
                } else if (temporal instanceof LocalTime) {
                    LocalTime lt = (LocalTime) temporal;
                    return java.sql.Time.valueOf(lt);
                } else {
                    throw new DateTimeParseException("Unsupported time type", value, 0);
                }
            } catch (DateTimeParseException e) {
            }
        }
        return null;
    }

    /**
     * Parses a binary value from a string.
     * Supports hexadecimal format (e.g., "48656C6C") and Base64 format.
     *
     * @param value the string to parse
     * @return byte array, or null if parsing fails
     */
    private byte[] parseBinary(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        value = value.trim();

        try {
            byte[] result;
            if (value.startsWith("0x") || value.startsWith("0X")) {
                try {
                    result = parseHexBinary(value.substring(2));
                } catch (IllegalArgumentException e) {
                    return null;
                }
            } else if (isHexFormat(value)) {
                try {
                    result = parseHexBinary(value);
                } catch (IllegalArgumentException e) {
                    return null;
                }
            } else {
                try {
                    result = parseBase64Binary(value);
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }

            if (result != null && result.length > MAX_BLOB_SIZE) {
                return null;
            }

            return result;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses a hexadecimal string to byte array.
     *
     * @param hex the hexadecimal string
     * @return byte array
     */
    private byte[] parseHexBinary(String hex) {
        int len = hex.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have even length");
        }
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);
            data[i / 2] = (byte) ((high << 4) + low);
        }
        return data;
    }

    /**
     * Parses a Base64 string to byte array.
     *
     * @param base64 the Base64 encoded string
     * @return byte array
     */
    private byte[] parseBase64Binary(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    /**
     * Writes a failed row to the .bad file with error message.
     * The error message is appended as an extra column.
     *
     * @param row          the failed CSV row
     * @param errorMessage the error message
     */
    private void writeBadRow(String[] row, String errorMessage) {
        try {
            if (errorMessage != null) {
                String error = errorMessage.trim();
                final int pos = error.indexOf("\n");
                error = error.substring(0, pos > 0 ? pos : error.length());
                badFileWriter.writeNext(new String[]{"[ERROR] " + error});
            }
            if (row != null) {
                badFileWriter.writeNext(row);
            }
        } catch (Exception e) {
            config.log("Error writing to bad file: " + e.getMessage());
        }
    }

    /**
     * Calculates the approximate byte size of a CSV row.
     * Used for progress tracking.
     *
     * @param row the CSV row
     * @return the approximate byte size
     */
    private long calculateRowSize(String[] row) {
        if (row == null) {
            return 0;
        }

        long size = 0;

        // Approximate overhead for CSV row: separator characters and newline
        // Each field is separated by a delimiter, plus a newline at the end
        size += row.length - 1 + 2; // 1 byte per delimiter, 2 bytes for CRLF

        // Calculate size for each field
        for (String field : row) {
            if (field != null) {
                // For simplicity, assume UTF-8 encoding (1-4 bytes per character)
                // Use 2 bytes as an average estimate
                int fieldLength = field.length();
                size += fieldLength * 2;

                // Add overhead for quotes if field contains special characters
                if (field.contains(String.valueOf(config.delimiter)) ||
                        field.contains(String.valueOf(config.quotechar)) ||
                        field.contains("\n") || field.contains("\r")) {
                    size += 2; // 2 bytes for quotes

                    // Add overhead for escaped characters
                    int escapeCount = 0;
                    for (char c : field.toCharArray()) {
                        if (c == config.escape || c == config.quotechar) {
                            escapeCount++;
                        }
                    }
                    size += escapeCount; // 1 byte per escaped character
                }
            } else {
                // For null values, assume minimal storage
                size += 2; // Minimal overhead for null
            }
        }

        return size;
    }

    /**
     * Logs progress information at regular intervals.
     * Displays timestamp, MB processed, rows processed, errors, and TPS.
     */
    private void logProgress() {
        long currentTime = System.currentTimeMillis();
        long intervalTime = currentTime - lastProgressTime;
        long intervalRows = (totalRowsProcessed - lastProgressRows) - (totalErrors - lastProgressErrors);
        long intervalBytes = totalBytesProcessed - lastProgressBytes;

        if (intervalBytes >= config.progressIntervalBytes) {
            double tps = intervalRows > 0 ? (intervalRows * 1000.0) / intervalTime : 0;
            double mbProcessed = totalBytesProcessed / (1024.0 * 1024.0);

            String message = String.format("[%s] Progress: %.2f MB processed, %d rows, %d successful, %d errors, %.2f successful/sec",
                    java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    mbProcessed,
                    totalRowsProcessed,
                    totalRowsProcessed - totalErrors,
                    totalErrors,
                    tps);

            config.log(message);

            lastProgressTime = currentTime;
            lastProgressRows = totalRowsProcessed;
            lastProgressBytes = totalBytesProcessed;
            lastProgressErrors = totalErrors;
        }
    }

    /**
     * Returns the total number of rows successfully processed.
     *
     * @return the total rows processed
     */
    public long getTotalRowsProcessed() {
        return totalRowsProcessed;
    }

    /**
     * Returns the total number of errors encountered.
     *
     * @return the total errors
     */
    public long getTotalErrors() {
        return totalErrors;
    }

    /**
     * Returns the path to the .bad file containing failed rows.
     *
     * @return the bad file path
     */
    public String getBadFilePath() {
        return badFilePath;
    }

    /**
     * Handles an error by incrementing error count and logging.
     * Can be overridden by subclasses for custom error handling.
     *
     * @param message the error message
     */
    protected void handleError(String message) {
        ++totalErrors;
        if (config.errors != -1 && totalErrors >= config.errors) {
            throw new RuntimeException("Error limit exceeded: " + totalErrors + " errors (max allowed: " + config.errors + ")");
        }
    }
}

