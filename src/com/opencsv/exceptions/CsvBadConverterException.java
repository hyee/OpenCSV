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
 * This exception indicates that the custom converter provided did not behave as
 * expected in some way.
 * Typically this means it could not be instantiated.
 *
 * @author Andrew Rucker Jones
 */
public class CsvBadConverterException extends RuntimeException {
    private final Class converterClass;

    /**
     * Default constructor, in case no further information is necessary or
     * available.
     */
    public CsvBadConverterException() {
        converterClass = null;
    }

    /**
     * Constructor for specifying the class of the offending converter.
     *
     * @param converterClass The class of the converter that misbehaved
     */
    public CsvBadConverterException(Class converterClass) {
        this.converterClass = converterClass;
    }

    /**
     * Constructor with a simple text.
     *
     * @param message Human-readable error text
     */
    public CsvBadConverterException(String message) {
        super(message);
        converterClass = null;
    }

    /**
     * Constructor for setting the class of the converter and an error message.
     *
     * @param converterClass Class of the converter that misbehaved
     * @param message        Human-readable error text
     */
    public CsvBadConverterException(Class converterClass, String message) {
        super(message);
        this.converterClass = converterClass;
    }

    /**
     * @return The class of the provided custom converter
     */
    public Class getConverterClass() {
        return converterClass;
    }
}
