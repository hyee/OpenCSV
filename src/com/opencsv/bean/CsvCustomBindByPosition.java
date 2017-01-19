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
 * Allows us to specify a class that will perform the translation from source
 * to destination.
 * For special needs, we can implement a class that takes the source field from
 * the CSV and translates it into a form of our choice.
 *
 * @author Andrew Rucker Jones
 * @since 3.8
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CsvCustomBindByPosition {

    /**
     * The class that takes care of the conversion.
     * Every custom converter must be descended from
     * {@link com.opencsv.bean.AbstractBeanField} and override the method
     * {@link com.opencsv.bean.AbstractBeanField#convert(java.lang.String)}.
     *
     * @return The implementation that can convert to the type of this field.
     */
    Class<? extends AbstractBeanField> converter();

    /**
     * The column position in the input that is used to fill the annotated
     * field.
     *
     * @return The position of the column in the CSV file from which this field
     * should be taken. This column number is zero-based.
     */
    int position();
}
