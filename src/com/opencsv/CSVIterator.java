package com.opencsv;

import java.io.IOException;
import java.util.Iterator;

/**
 * Provides an Iterator over the data found in opencsv.
 * <p>
 * <b>Important:</b> This iterator does not automatically close the underlying CSVReader.
 * Users should close the CSVReader manually after finishing iteration:
 * <pre>
 * try (CSVReader reader = new CSVReader(...)) {
 *     Iterator<String[]> iterator = reader.iterator();
 *     while (iterator.hasNext()) {
 *         String[] line = iterator.next();
 *         // process line
 *     }
 * }
 * </pre>
 */
public class CSVIterator implements Iterator<String[]> {
    private CSVReader reader;
    private String[] nextLine;

    /**
     * @param reader reader for the csv data.
     * @throws IOException              if unable to read data from the reader.
     * @throws IllegalArgumentException if reader is null
     */
    public CSVIterator(CSVReader reader) throws IOException {
        if (reader == null) {
            throw new IllegalArgumentException("CSVReader cannot be null");
        }
        this.reader = reader;
        nextLine = reader.readNext();
    }

    /**
     * Returns true if the iteration has more elements.
     * In other words, returns true if next() would return an element rather than throwing an exception.
     *
     * @return true if the CSVIterator has more elements.
     */
    public boolean hasNext() {
        return nextLine != null;
    }

    /**
     * Returns the next elenebt in the iterator.
     *
     * @return The next element of the iterator.
     * @throws RuntimeException if unable to read data from the reader
     */
    public String[] next() {
        String[] temp = nextLine;
        try {
            nextLine = reader.readNext();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next line from CSV", e);
        }
        return temp;
    }

    /**
     * This method is not supported by openCSV and will throw a UnsupportedOperationException if called.
     */
    public void remove() {
        throw new UnsupportedOperationException("This is a read only iterator.");
    }
}
