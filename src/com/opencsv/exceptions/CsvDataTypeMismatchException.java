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
 * This exception should be thrown when the provided string value for conversion
 * cannot be converted to the required type of the destination field.
 *
 * @author Andrew Rucker Jones
 */
public class CsvDataTypeMismatchException extends CsvException {
    private final Object sourceObject;
    private final Class destinationClass;

    /**
     * Default constructor, in case no further information is necessary or
     * available.
     */
    public CsvDataTypeMismatchException() {
        sourceObject = null;
        destinationClass = null;
    }

    /**
     * Constructor for setting the data and the class of the intended
     * destination field.
     *
     * @param sourceObject     Object that was to be assigned to the destination field
     * @param destinationClass Class of the destination field
     */
    public CsvDataTypeMismatchException(Object sourceObject, Class destinationClass) {
        this.sourceObject = sourceObject;
        this.destinationClass = destinationClass;
    }

    /**
     * Constructor with a simple text.
     *
     * @param message Human-readable error text
     */
    public CsvDataTypeMismatchException(String message) {
        super(message);
        sourceObject = null;
        destinationClass = null;
    }

    /**
     * Constructor for setting the data and the class of the intended
     * destination field along with an error message.
     *
     * @param sourceObject     Object that was to be assigned to the destination field
     * @param destinationClass Class of the destination field
     * @param message          Human-readable error text
     */
    public CsvDataTypeMismatchException(Object sourceObject, Class destinationClass, String message) {
        super(message);
        this.sourceObject = sourceObject;
        this.destinationClass = destinationClass;
    }

    /**
     * Gets the object that was to be assigned to a field of the wrong type.
     *
     * @return The data that could not be assigned
     */
    public Object getSourceObject() {
        return sourceObject;
    }

    /**
     * Gets the type of the field to which the data were to be assigned.
     *
     * @return The class of the destination field
     */
    public Class getDestinationClass() {
        return destinationClass;
    }
}
