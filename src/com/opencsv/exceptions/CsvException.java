/*
 * Copyright 2016 Andrew Rucker Jones.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opencsv.exceptions;

/**
 * This is the base class for all exceptions for opencsv.
 *
 * @author Andrew Rucker Jones
 */
public class CsvException extends Exception {
    private long lineNumber;

    /**
     * Default constructor, in case no parameters are required.
     */
    public CsvException() {
        lineNumber = -1;
    }

    /**
     * Constructor that allows a human-readable message.
     *
     * @param message The error text
     */
    public CsvException(String message) {
        super(message);
        lineNumber = -1;
    }

    /**
     * @return The line number that caused the error. This should be the
     * one-based number of the line that caused the error, not including the
     * header line, if present.
     */
    public long getLineNumber() {
        return lineNumber;
    }

    /**
     * @param lineNumber The line number that caused the error. This should be the
     *                   one-based number of the line that caused the error, not including the
     *                   header line, if present.
     */
    public void setLineNumber(long lineNumber) {
        this.lineNumber = lineNumber;
    }
}
