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

import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.converters.*;
import org.apache.commons.beanutils.locale.converters.*;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;

/**
 * This class wraps fields from the reflection API in order to handle
 * translation of primitive types and to add a "required" flag.
 *
 * @param <T> The type of the bean
 * @author Andrew Rucker Jones
 */
public class BeanFieldPrimitiveTypes<T> extends AbstractBeanField<T> {

    private final boolean required;
    private final String locale;

    /**
     * @param field    A {@link java.lang.reflect.Field} object.
     * @param required True if the field is required to contain a value, false
     *                 if it is allowed to be null or a blank string.
     * @param locale   If not null or empty, specifies the locale used for
     *                 converting locale-specific data types
     */
    public BeanFieldPrimitiveTypes(Field field, boolean required, String locale) {
        super(field);
        this.required = required;
        this.locale = locale;
    }

    /**
     * @return True if the field is required to be set (cannot be null or an
     * empty string), false otherwise.
     */
    public boolean isRequired() {
        return this.required;
    }

    @Override
    protected Object convert(String value) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
        if (required && StringUtils.isBlank(value)) {
            throw new CsvRequiredFieldEmptyException(String.format("Field '%s' is mandatory but no value was provided.", field.getName()));
        }

        Object o = null;

        if (StringUtils.isNotBlank(value)) {
            Class<?> fieldType = field.getType();
            try {
                if (fieldType.equals(Boolean.TYPE) || fieldType.equals(Boolean.class)) {
                    BooleanConverter c = new BooleanConverter();
                    o = c.convert(Boolean.class, value.trim());
                } else if (fieldType.equals(Byte.TYPE) || fieldType.equals(Byte.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        ByteConverter c = new ByteConverter();
                        o = c.convert(Byte.class, value.trim());
                    } else {
                        ByteLocaleConverter c = new ByteLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(Double.TYPE) || fieldType.equals(Double.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        DoubleConverter c = new DoubleConverter();
                        o = c.convert(Double.class, value.trim());
                    } else {
                        DoubleLocaleConverter c = new DoubleLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(Float.TYPE) || fieldType.equals(Float.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        FloatConverter c = new FloatConverter();
                        o = c.convert(Float.class, value.trim());
                    } else {
                        FloatLocaleConverter c = new FloatLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(Integer.TYPE) || fieldType.equals(Integer.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        IntegerConverter c = new IntegerConverter();
                        o = c.convert(Integer.class, value.trim());
                    } else {
                        IntegerLocaleConverter c = new IntegerLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(Long.TYPE) || fieldType.equals(Long.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        LongConverter c = new LongConverter();
                        o = c.convert(Long.class, value.trim());
                    } else {
                        LongLocaleConverter c = new LongLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(Short.TYPE) || fieldType.equals(Short.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        ShortConverter c = new ShortConverter();
                        o = c.convert(Short.class, value.trim());
                    } else {
                        ShortLocaleConverter c = new ShortLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(Character.TYPE) || fieldType.equals(Character.class)) {
                    CharacterConverter c = new CharacterConverter();
                    o = c.convert(Character.class, value.charAt(0));
                } else if (fieldType.equals(BigDecimal.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        BigDecimalConverter c = new BigDecimalConverter();
                        o = c.convert(BigDecimal.class, value.trim());
                    } else {
                        BigDecimalLocaleConverter c = new BigDecimalLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.equals(BigInteger.class)) {
                    if (StringUtils.isEmpty(locale)) {
                        BigIntegerConverter c = new BigIntegerConverter();
                        o = c.convert(BigInteger.class, value.trim());
                    } else {
                        BigIntegerLocaleConverter c = new BigIntegerLocaleConverter(Locale.forLanguageTag(locale));
                        o = c.convert(value.trim());
                    }
                } else if (fieldType.isAssignableFrom(String.class)) {
                    o = value;
                } else {
                    throw new CsvDataTypeMismatchException(value, fieldType, String.format("Unable to set field value for field '%s' with value '%s' " + "- type is unsupported. Use primitive, boxed " + "primitive, BigDecimal, BigInteger and String types only.", fieldType, value));
                }
            } catch (ConversionException e) {
                CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(value, fieldType);
                csve.initCause(e);
                throw csve;
            }
        }
        return o;
    }
}
