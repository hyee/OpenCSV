package com.opencsv;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * FileReader utility class for handling files, including compressed files.
 */
public class FileReader {

    /**
     * Get list of files from the given file path.
     * If the file is a compressed file (zip, jar, gz), returns the files inside it.
     * Otherwise, returns a list containing only the given file.
     *
     * @param filePath the path to the file
     * @return array of file paths
     * @throws IOException if the file does not exist or is not readable
     */
    public static String[] getFiles(String filePath) throws IOException {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        File file = new File(filePath);

        // Check if file exists and is readable
        if (!file.exists()) {
            throw new IOException("File does not exist: " + filePath);
        }

        if (!file.canRead()) {
            throw new IOException("File is not readable: " + filePath);
        }

        // Check if file is a compressed file
        String fileName = file.getName().toLowerCase();
        if (fileName.endsWith(".zip") || fileName.endsWith(".jar")) {
            return getFilesFromZip(filePath);
        } else if (fileName.endsWith(".gz")) {
            // Check if it's a tar.gz file
            if (fileName.endsWith(".tar.gz")) {
                throw new IOException("Unsupported compressed file type: tar.gz");
            }
            // For gz files (not tar.gz), return the original file path
            // as gz usually contains a single file
            return new String[]{filePath};
        } else {
            // For non-compressed files, return a single-element array
            return new String[]{filePath};
        }
    }

    /**
     * Get files from a zip or jar file.
     *
     * @param zipFilePath the path to the zip or jar file
     * @return array of file paths inside the zip file, sorted by file time and then by file name
     * @throws IOException if there's an error reading the zip file
     */
    private static String[] getFilesFromZip(String zipFilePath) throws IOException {
        List<ZipEntryInfo> entryList = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(zipFilePath);
             ZipInputStream zis = new ZipInputStream(fis)) {

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                // Skip directories
                if (!entry.isDirectory()) {
                    // Store entry information for sorting
                    entryList.add(new ZipEntryInfo(zipFilePath, entry));
                }
                zis.closeEntry();
            }
        }

        // Sort entries by last modified time (descending), then by name (ascending)
        entryList.sort((e1, e2) -> {
            long time1 = e1.getLastModifiedTime();
            long time2 = e2.getLastModifiedTime();

            // Handle case where modification time is -1 (not set)
            if (time1 == -1 && time2 == -1) {
                // Both have no modification time, sort by name
                return e1.getName().compareToIgnoreCase(e2.getName());
            } else if (time1 == -1) {
                // e1 has no modification time, place it after e2
                return 1;
            } else if (time2 == -1) {
                // e2 has no modification time, place it after e1
                return -1;
            } else {
                // Both have modification times, sort by time descending
                long timeDiff = time2 - time1;
                if (timeDiff != 0) {
                    return Long.compare(timeDiff, 0);
                }
                // Times are equal, sort by name (case-insensitive)
                return e1.getName().compareToIgnoreCase(e2.getName());
            }
        });

        // Check if no files were found in the zip
        if (entryList.isEmpty()) {
            throw new IOException("No files found in zip/jar file: " + zipFilePath);
        }

        // Convert to array of file paths
        String[] filePaths = new String[entryList.size()];
        for (int i = 0; i < entryList.size(); i++) {
            filePaths[i] = entryList.get(i).getFilePath();
        }

        return filePaths;
    }

    /**
     * Helper class to store zip entry information for sorting.
     */
    private static class ZipEntryInfo {
        private final String zipFilePath;
        private final String entryName;
        private final long lastModifiedTime;

        public ZipEntryInfo(String zipFilePath, ZipEntry entry) {
            this.zipFilePath = zipFilePath;
            this.entryName = entry.getName();
            this.lastModifiedTime = entry.getTime();
        }

        public String getFilePath() {
            return zipFilePath + "!" + entryName;
        }

        public String getName() {
            return entryName;
        }

        public long getLastModifiedTime() {
            return lastModifiedTime;
        }
    }

    /**
     * Check if a file is a compressed file.
     *
     * @param filePath the path to the file
     * @return true if the file is a compressed file, false otherwise
     */
    public static boolean isCompressedFile(String filePath) {
        if (filePath == null) {
            return false;
        }

        String fileName = filePath.toLowerCase();
        return fileName.endsWith(".zip") || fileName.endsWith(".jar") || fileName.endsWith(".gz");
    }

    /**
     * Check if a file path represents a zip entry.
     *
     * @param filePath the path to check
     * @return true if the path represents a zip entry, false otherwise
     */
    public static boolean isZipEntry(String filePath) {
        return filePath != null && filePath.contains("!");
    }

    /**
     * Get the zip file path from a zip entry path.
     *
     * @param zipEntryPath the zip entry path
     * @return the zip file path
     */
    public static String getZipFilePath(String zipEntryPath) {
        if (zipEntryPath == null || !zipEntryPath.contains("!")) {
            return zipEntryPath;
        }
        return zipEntryPath.split("!")[0];
    }

    /**
     * Get the entry name from a zip entry path.
     *
     * @param zipEntryPath the zip entry path
     * @return the entry name
     */
    public static String getZipEntryName(String zipEntryPath) {
        if (zipEntryPath == null || !zipEntryPath.contains("!")) {
            return null;
        }
        return zipEntryPath.substring(zipEntryPath.indexOf("!") + 1);
    }

    /**
     * Create a Reader for the given file path, supporting compressed files.
     *
     * @param filePath the path to the file
     * @param encoding the encoding to use
     * @return a Reader for the file
     * @throws IOException if there's an error creating the reader
     */
    public static java.io.Reader createReader(String filePath, String encoding) throws IOException {
        if (isZipEntry(filePath)) {
            // Handle zip entry
            String zipFilePath = getZipFilePath(filePath);
            String entryName = getZipEntryName(filePath);

            FileInputStream fis = new FileInputStream(zipFilePath);
            ZipInputStream zis = new ZipInputStream(fis);

            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().equals(entryName)) {
                    if (encoding == null || encoding.trim().isEmpty()) {
                        return new InputStreamReader(zis);
                    } else {
                        return new InputStreamReader(zis, encoding);
                    }
                }
                zis.closeEntry();
            }

            zis.close();
            throw new IOException("Entry not found in zip file: " + entryName);
        } else if (filePath.toLowerCase().endsWith(".gz")) {
            // Handle gz file
            FileInputStream fis = new FileInputStream(filePath);
            GZIPInputStream gzis = new GZIPInputStream(fis);

            if (encoding == null || encoding.trim().isEmpty()) {
                return new InputStreamReader(gzis);
            } else {
                return new InputStreamReader(gzis, encoding);
            }
        } else {
            // Handle regular file
            if (encoding == null || encoding.trim().isEmpty()) {
                return new java.io.FileReader(filePath);
            } else {
                return new InputStreamReader(new FileInputStream(filePath), encoding);
            }
        }
    }
}
