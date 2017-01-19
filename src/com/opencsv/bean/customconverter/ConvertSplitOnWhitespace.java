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
package com.opencsv.bean.customconverter;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import com.opencsv.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class takes a string and splits it on whitespace into a list of strings.
 *
 * @author Andrew Rucker Jones
 */
public class ConvertSplitOnWhitespace extends AbstractBeanField {

    /**
     * Takes a string that is a list of substrings separated by whitespace and
     * returns a list of the substrings.
     * For example, the string "Jones Smith Cartwright Cooper" would be
     * converted by this method to ("Jones", "Smith", "Cartwright", "Cooper").
     * When might this be useful? A CSV has a set number of columns, but
     * sometimes some fields might need to contain a variable number of entries.
     * In this case, a list within one field of a CSV might make sense.
     *
     * @param value The string to be converted
     * @return List&lt;String&gt; consisting of the substrings from the input
     * that were separated in the input by whitespace
     * @throws CsvDataTypeMismatchException   Is not thrown by this implementation
     * @throws CsvRequiredFieldEmptyException Is not thrown by this implementation
     */
    @Override
    protected Object convert(String value) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
        List<String> l = null;
        if (!StringUtils.isEmpty(value)) {
            l = new ArrayList<String>(Arrays.asList(value.split("\\s+")));
        }
        return l;
    }

}
