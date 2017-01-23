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
package com.opencsv.bean;

import java.lang.annotation.*;

/**
 * This annotation indicates that the destination field is an expression of time.
 * Conversion to the following types is supported:
 * <ul><li>{@link java.util.Date}</li>
 * <li>{@link java.util.Calendar} (a {@link java.util.GregorianCalendar} is returned)</li>
 * <li>{@link java.util.GregorianCalendar}</li>
 * <li>{@link javax.xml.datatype.XMLGregorianCalendar}</li>
 * <li>{@link java.sql.Date}</li>
 * <li>{@link java.sql.Time}</li>
 * <li>{@link java.sql.Timestamp}</li>
 * </ul>
 * This annotation must be used with either {@link com.opencsv.bean.CsvBindByName}
 * or {@link com.opencsv.bean.CsvBindByPosition}, otherwise it is ignored.
 *
 * @author Andrew Rucker Jones
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CsvDate {
    /**
     * A time format string.
     * This must be a string understood by
     * {@link java.time.format.DateTimeFormatter#ofPattern(java.lang.String)}.
     * The default value conforms with
     * <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> and is
     * {@code "yyyyMMdd'T'HHmmss"}. Locale information, if specified, is gleaned
     * from one of the other CSV-related annotations and is used for conversion.
     *
     * @return The format string for parsing input
     */
    String value() default "yyyyMMdd'T'HHmmss";
}
