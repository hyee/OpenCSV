package com.opencsv;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Locale;
import java.util.Map;

/**
 * DBLoaderConfig - Configuration class for DBLoader.
 * This class encapsulates all the configuration parameters for DBLoader,
 * providing a cleaner and more maintainable way to configure the loader.
 */
public class DBLoaderConfig {
    // Configuration option constants
    /**
     * Configuration option for the number of rows to scan for type detection.
     */
    public static final String SCAN_ROWS = "SCAN_ROWS";

    /**
     * Configuration option for column size mode (ACTUAL or MAXIMUM).
     */
    public static final String COLUMN_SIZE = "COLUMN_SIZE";

    /**
     * Configuration option for date format string.
     */
    public static final String DATE_FORMAT = "DATE_FORMAT";

    /**
     * Configuration option for timestamp format string.
     */
    public static final String TIMESTAMP_FORMAT = "TIMESTAMP_FORMAT";

    /**
     * Configuration option for timestamp with timezone format string.
     */
    public static final String TIMESTAMPTZ_FORMAT = "TIMESTAMPTZ_FORMAT";

    /**
     * Configuration option for locale to use for date and timestamp parsing.
     */
    public static final String LOCALE = "LOCALE";

    /**
     * Configuration option for mapping CSV column names to database column names.
     */
    public static final String MAP_COLUMN_NAMES = "MAP_COLUMN_NAMES";

    /**
     * Value for COLUMN_SIZE option to use actual column size from data.
     */
    public static final String COLUMN_SIZE_ACTUAL = "ACTUAL";

    /**
     * Value for COLUMN_SIZE option to use maximum column size.
     */
    public static final String COLUMN_SIZE_MAXIMUM = "MAXIMUM";

    /**
     * Configuration option for controlling what to display.
     */
    public static final String SHOW = "SHOW";

    /**
     * Configuration option for generating DDL and creating table.
     */
    public static final String CREATE = "CREATE";

    /**
     * Configuration option for truncating target table.
     */
    public static final String TRUNCATE = "TRUNCATE";

    /**
     * Configuration option for maximum number of errors allowed.
     */
    public static final String ERRORS = "ERRORS";

    /**
     * Configuration option for batch size.
     */
    public static final String BATCH_ROWS = "BATCH_ROWS";

    /**
     * Configuration option for database platform.
     */
    public static final String PLATFORM = "PLATFORM";

    /**
     * Configuration option for row limit.
     */
    public static final String ROW_LIMIT = "ROW_LIMIT";

    /**
     * Configuration option for delimiter character.
     */
    public static final String DELIMITER = "DELIMITER";

    /**
     * Configuration option for enclosure character.
     */
    public static final String ENCLOSURE = "ENCLOSURE";

    /**
     * Configuration option for escape character.
     */
    public static final String ESCAPE = "ESCAPE";

    /**
     * Configuration option for unescaping newlines in CSV data.
     */
    public static final String UNESCAPE_NEWLINE = "UNESCAPE_NEWLINE";

    /**
     * Configuration option for number of lines to skip.
     */
    public static final String SKIP_ROWS = "SKIP_ROWS";
    /**
     * Configuration option for number of columns to skip. Avail values: "auto","off","(<column1>,<column2>,...)".
     */
    public static final String SKIP_COLUMNS = "SKIP_COLUMNS";

    /**
     * Configuration option for progress reporting interval in MB.
     */
    public static final String REPORT_MB = "REPORT_MB";

    /**
     * Configuration option for encoding.
     */
    public static final String ENCODING = "ENCODING";

    /**
     * Configuration option for variable placeholder format.
     */
    public static final String VARIABLE_FORMAT = "VARIABLE_FORMAT";

    /**
     * Configuration option for whether the CSV file has a header row.
     */
    public static final String HAS_HEADER = "HAS_HEADER";

    public static final String LOGGER = "LOGGER";
    public final static String SKIP_COLUMNS_AUTO = "__AUTO__";
    /**
     * Whether the CSV file has a header row.
     */
    public boolean hasHeader = true;
    /**
     * Number of rows to batch before committing.
     */
    public int batchSize;
    /**
     * Byte interval for progress logging.
     */
    public long progressIntervalBytes;
    /**
     * Maximum number of rows to process (0 for no limit).
     */
    public int rowLimit;
    /**
     * Character to use for separating entries.
     */
    public char delimiter;
    /**
     * Character to use for quoted elements.
     */
    public char quotechar;
    /**
     * Character to use for escaping a separator or quote.
     */
    public char escape;
    /**
     * Number of lines to skip at start of CSV file.
     */
    public int skipLines;
    /**
     * Charset to use for reading the CSV file (null/empty/"auto" for auto-detection).
     */
    public String encoding;
    /**
     * Map of CSV column names to database column names.
     */
    public Map<String, String> columnNameMap;
    /**
     * Number of rows to scan for type detection.
     */
    public int scanRows;
    /**
     * Column size mode (ACTUAL or MAXIMUM).
     */
    public String columnSize;
    /**
     * Date format string for parsing dates.
     */
    public String dateFormat;
    /**
     * Timestamp format string for parsing timestamps.
     */
    public String timestampFormat;
    /**
     * Timestamp with timezone format string for parsing timestamps with timezone.
     */
    public String timestamptzFormat;
    /**
     * Controls what to display.
     */
    public String show;
    /**
     * Whether to generate DDL and create table.
     */
    public boolean create;
    /**
     * Whether to truncate target table.
     */
    public boolean truncate;
    /**
     * Maximum number of errors allowed.
     */
    public int errors;
    /**
     * Locale to use for date and timestamp parsing.
     */
    public String localeName;
    public Locale locale;
    /**
     * Database platform.
     */
    public String platform;
    public boolean unescapeNewline;
    /**
     * Variable placeholder format. : or ?
     */
    public char variableFormat;
    public Map<String, String> skipColumns = new java.util.HashMap<>();
    /**
     * SQL query to retrieve column informations,if not specified then Connection.getMetaData().getColumns() will be used.
     * The query must return the following columns, it accepts no bind variables:
     * - COLUMN_NAME(String): The name of the column.
     * - DATA_TYPE(int): The data type of the column(Refer to java.sql.Types).
     * - TYPE_NAME(String): The name of the data type.
     * - COLUMN_SIZE(int): The size of the column.
     */
    public String columnInfoSQL;
    public String[] tableColumns;
    public Integer[] columnTypes;
    public String[] columnTypeNames;
    public Integer[] columnSizes;
    public Integer[] csvColumns;
    public String[] csvHeaders;
    public OutputStream logger;

    /**
     * Default constructor with default values.
     */
    public DBLoaderConfig() {
        this.hasHeader = true;
        this.batchSize = 2048;
        this.progressIntervalBytes = 10 * 1024 * 1024;
        this.rowLimit = 0;
        this.delimiter = CSVParser.DEFAULT_SEPARATOR;
        this.quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
        this.escape = CSVParser.DEFAULT_ESCAPE_CHARACTER;
        this.skipLines = CSVReader.DEFAULT_SKIP_LINES;
        this.encoding = null;
        this.columnNameMap = null;
        this.scanRows = 200;
        this.columnSize = COLUMN_SIZE_MAXIMUM;
        this.locale = Locale.getDefault();
        this.localeName = "";
        this.dateFormat = null;
        this.timestampFormat = null;
        this.timestamptzFormat = null;
        this.show = "OFF";
        this.create = false;
        this.truncate = false;
        this.errors = -1;
        this.platform = null;
        this.variableFormat = '?';
        this.skipColumns.put(SKIP_COLUMNS_AUTO, "true");
        this.unescapeNewline = true;
        this.logger = System.out;
    }

    /**
     * Parses a map of options and creates a DBLoaderConfig instance.
     * <p>
     * This method parses all configuration options as specified in the Javadoc for
     * {@link #importCSVData(Connection, String, String, Map)} and {@link #generateInsertStatement(Connection, String, String, Map)}.
     * All option names and string values are case-insensitive.
     *
     * @param options configuration options map
     * @return DBLoaderConfig instance with settings from options
     */
    public static DBLoaderConfig parseOptions(Map<String, Object> options) {
        DBLoaderConfig config = new DBLoaderConfig();

        if (options != null) {
            // Parse hasHeader option - true if CSV file has a header row
            Boolean hasHeaderOption = getOptionBooleanFromMap(options, HAS_HEADER, null);
            if (hasHeaderOption != null) {
                config.hasHeader = hasHeaderOption;
            }

            // Parse batch size option - number of rows to batch before committing
            Integer batchSizeOption = getOptionIntFromMap(options, BATCH_ROWS, null);
            if (batchSizeOption != null) {
                config.batchSize = batchSizeOption;
            }

            // Parse row limit option - maximum number of rows to process
            Integer rowLimitOption = getOptionIntFromMap(options, ROW_LIMIT, null);
            if (rowLimitOption != null) {
                config.rowLimit = rowLimitOption;
            }

            // Parse progress interval option - byte interval for progress logging
            Integer reportMbOption = getOptionIntFromMap(options, REPORT_MB, null);
            if (reportMbOption != null) {
                config.progressIntervalBytes = (long) reportMbOption * 1024 * 1024;
            }

            // Parse delimiter option - column delimiter for CSV parsing
            Character delimiterOption = getOptionCharFromMap(options, DELIMITER, null);
            if (delimiterOption != null) {
                config.delimiter = delimiterOption;
            }

            // Parse quote character option - enclosure character for CSV parsing
            Character quotecharOption = getOptionCharFromMap(options, ENCLOSURE, null);
            if (quotecharOption != null) {
                config.quotechar = quotecharOption;
            }

            // Parse escape character option - escape character for CSV parsing
            Character escapeOption = getOptionCharFromMap(options, ESCAPE, null);
            if (escapeOption != null) {
                config.escape = escapeOption;
            }

            // Parse unescape newline option - true if newline characters should be unescaped
            Boolean unescapeNewlineOption = getOptionBooleanFromMap(options, UNESCAPE_NEWLINE, null);
            if (unescapeNewlineOption != null) {
                config.unescapeNewline = unescapeNewlineOption;
            }

            // Parse skip lines option - number of lines to skip at start of CSV file
            Integer skipLinesOption = getOptionIntFromMap(options, SKIP_ROWS, null);

            if (skipLinesOption != null) {
                config.skipLines = skipLinesOption;
            } else if (config.hasHeader) {
                config.skipLines = 1;
            }

            // Parse encoding option - charset to use for reading CSV file
            String encodingOption = getOptionStringFromMap(options, ENCODING, null);
            if (encodingOption != null && !encodingOption.isEmpty() && !encodingOption.equalsIgnoreCase("auto")) {
                config.encoding = encodingOption;
            }

            // Parse column name mapping option - map of CSV column names to database column names
            Map<String, String> columnNameMapOption = getOptionMapFromMap(options, MAP_COLUMN_NAMES, null);
            if (columnNameMapOption != null) {
                config.columnNameMap = columnNameMapOption;
            }

            // Parse additional options and store them in DBLoaderConfig
            // SCAN_ROWS - used for type detection
            Integer scanRowsOption = getOptionIntFromMap(options, SCAN_ROWS, null);
            if (scanRowsOption != null) {
                config.scanRows = scanRowsOption;
            }
            // COLUMN_SIZE - used for column size mode (ACTUAL or MAXIMUM)
            String columnSizeOption = getOptionStringFromMap(options, COLUMN_SIZE, null);
            if (columnSizeOption != null) {
                config.columnSize = columnSizeOption;
            }

            // LOCALE - used for locale to use for date and timestamp parsing
            String localeOption = getOptionStringFromMap(options, LOCALE, null);
            if (localeOption != null) {
                config.localeName = localeOption;
                config.locale = localeOption != null && !localeOption.isEmpty() && !localeOption.equalsIgnoreCase("auto") ? Locale.forLanguageTag(localeOption) : Locale.getDefault();
            }
            // DATE_FORMAT - used for date parsing
            String dateFormatOption = getOptionStringFromMap(options, DATE_FORMAT, null);
            if (dateFormatOption != null) {
                config.dateFormat = dateFormatOption;
            }
            // TIMESTAMP_FORMAT - used for timestamp parsing
            String timestampFormatOption = getOptionStringFromMap(options, TIMESTAMP_FORMAT, null);
            if (timestampFormatOption != null) {
                config.timestampFormat = timestampFormatOption;
            }
            // TIMESTAMPTZ_FORMAT - used for timestamp with timezone parsing
            String timestamptzFormatOption = getOptionStringFromMap(options, TIMESTAMPTZ_FORMAT, null);
            if (timestamptzFormatOption != null) {
                config.timestamptzFormat = timestamptzFormatOption;
            }
            // SHOW - used for controlling display (DDL, DML, ALL)
            String showOption = getOptionStringFromMap(options, SHOW, null);
            if (showOption != null) {
                config.show = showOption;
            }
            // CREATE - used for generating DDL and creating table
            Boolean createOption = getOptionBooleanFromMap(options, CREATE, null);
            if (createOption != null) {
                config.create = createOption;
            }
            // TRUNCATE - used for truncating target table
            Boolean truncateOption = getOptionBooleanFromMap(options, TRUNCATE, null);
            if (truncateOption != null) {
                config.truncate = truncateOption;
            }
            // ERRORS - used for maximum number of errors allowed
            Integer errorsOption = getOptionIntFromMap(options, ERRORS, null);
            if (errorsOption != null) {
                config.errors = errorsOption;
            }
            // PLATFORM - used for database platform (oracle, mysql, db2, mssql, pgsql)
            String platformOption = getOptionStringFromMap(options, PLATFORM, null);
            if (platformOption != null && !platformOption.isEmpty() && !platformOption.equalsIgnoreCase("auto")) {
                config.platform = platformOption;
            }
            // VARIABLE_FORMAT - used for variable placeholder format ("?" or ":")
            String variableFormatOption = getOptionStringFromMap(options, VARIABLE_FORMAT, null);
            if (variableFormatOption != null) {
                config.variableFormat = variableFormatOption.charAt(0);
            }
            config.columnInfoSQL = getOptionStringFromMap(options, "COLUMN_INFO_SQL", null);

            Object loggerOption = getOptionValueFromMap(options, LOGGER);
            if (loggerOption != null && loggerOption instanceof OutputStream) {
                config.logger = (OutputStream) loggerOption;
            }

            // Parse skip columns option - columns to skip (auto, off, or list of column names)
            String skipColumnsOption = getOptionStringFromMap(options, SKIP_COLUMNS, null);
            if (skipColumnsOption != null) {
                skipColumnsOption = skipColumnsOption.trim();
                if (skipColumnsOption.equalsIgnoreCase("off")) {
                    config.skipColumns.clear();
                } else if (skipColumnsOption.startsWith("(") && skipColumnsOption.endsWith(")")) {
                    skipColumnsOption = skipColumnsOption.substring(1, skipColumnsOption.length() - 1);
                    String[] skipColumns = skipColumnsOption.split(",");
                    for (String skipColumn : skipColumns) {
                        if (!skipColumn.trim().isEmpty()) {
                            config.skipColumns.put(skipColumn.trim().toUpperCase(), "true");
                        }
                    }
                } else if (skipColumnsOption.equalsIgnoreCase("auto")) {
                    config.skipColumns.put(SKIP_COLUMNS_AUTO, "true");
                } else {
                    throw new IllegalArgumentException("Invalid value for SKIP_COLUMNS option. Expected 'off', 'auto', or a list of column names enclosed in parentheses.");
                }
            }
        }
        return config;
    }

    // Helper methods for parsing options from map
    private static Boolean getOptionBooleanFromMap(Map<String, Object> options, String key, Boolean defaultValue) {
        Object value = getOptionValueFromMap(options, key);
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        String strValue = value.toString().toLowerCase().trim();
        return "true".equals(strValue) || "1".equals(strValue) || "yes".equals(strValue) || "y".equals(strValue) || "on".equals(strValue);
    }

    private static Integer getOptionIntFromMap(Map<String, Object> options, String key, Integer defaultValue) {
        Object value = getOptionValueFromMap(options, key);
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof Integer) {
            return (Integer) value;
        }

        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static Character getOptionCharFromMap(Map<String, Object> options, String key, Character defaultValue) {
        Object value = getOptionValueFromMap(options, key);
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof Character) {
            return (Character) value;
        }

        String strValue = value.toString();
        if (strValue.isEmpty()) {
            return defaultValue;
        }

        return strValue.charAt(0);
    }

    private static String getOptionStringFromMap(Map<String, Object> options, String key, String defaultValue) {
        Object value = getOptionValueFromMap(options, key);
        if (value == null) {
            return defaultValue;
        }

        return value.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getOptionMapFromMap(Map<String, Object> options, String key, Map<String, String> defaultValue) {
        Object value = getOptionValueFromMap(options, key);
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof Map) {
            return (Map<String, String>) value;
        }

        return defaultValue;
    }

    /**
     * Gets an option value from the options map, ignoring case.
     *
     * @param options the options map
     * @param key     the option key
     * @return the option value, or null if not found
     */
    private static Object getOptionValueFromMap(Map<String, Object> options, String key) {
        if (options == null) {
            return null;
        }

        for (Map.Entry<String, Object> entry : options.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public void log(String message) {
        if (progressIntervalBytes < 0) {
            return;
        }
        if (logger.equals(System.out)) {
            message = "    " + (message.trim().replaceAll("\n", "\n    "));
        }
        try {
            logger.write((message + "\n").getBytes());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write log message", e);
        }
    }
}