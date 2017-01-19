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
import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.converters.BooleanConverter;
import org.apache.commons.lang3.StringUtils;

/**
 * This class converts common German representations of boolean values into a
 * Boolean.
 * This class also demonstrates how to localize booleans for any other language.
 *
 * @author Andrew Rucker Jones
 */
public class ConvertGermanToBoolean extends AbstractBeanField {

    /**
     * Converts German text into a Boolean.
     * The comparisons are case-insensitive. The recognized pairs are
     * wahr/falsch, w/f, ja/nein, j/n, 1/0.
     *
     * @param value String that should represent a Boolean
     * @return Boolean
     * @throws CsvDataTypeMismatchException   If anything other than the
     *                                        explicitly translated pairs is found
     * @throws CsvRequiredFieldEmptyException Is not thrown by this
     *                                        implementation. See {@link com.opencsv.bean.customconverter.ConvertGermanToBooleanRequired}.
     */
    @Override
    protected Object convert(String value) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        String[] trueStrings = {"wahr", "ja", "j", "1", "w"};
        String[] falseStrings = {"falsch", "nein", "n", "0", "f"};
        Converter bc = new BooleanConverter(trueStrings, falseStrings);
        try {
            return bc.convert(Boolean.class, value.trim());
        } catch (ConversionException e) {
            CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(value, field.getType(), "Eingabe war kein boolischer Wert.");
            csve.initCause(e);
            throw csve;
        }
    }
}
