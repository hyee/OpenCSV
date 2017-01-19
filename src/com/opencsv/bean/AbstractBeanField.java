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

import com.opencsv.exceptions.CsvConstraintViolationException;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This base bean takes over the responsibility of converting the supplied
 * string to the proper type for the destination field and setting the
 * destination field. All custom converters must be descended from this class.
 *
 * @param <T> Type of the bean being populated
 * @author Andrew Rucker Jones
 */
abstract public class AbstractBeanField<T> implements BeanField<T> {

    protected Field field;

    /**
     * Default nullary constructor, so derived classes aren't forced to create
     * a constructor with one Field parameter.
     */
    public AbstractBeanField() {

    }

    /**
     * @param field A java.lang.reflect.Field object.
     */
    public AbstractBeanField(Field field) {
        this.field = field;
    }

    @Override
    public void setField(Field field) {
        this.field = field;
    }

    @Override
    public Field getField() {
        return this.field;
    }

    @Override
    public final <T> void setFieldValue(T bean, String value) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException, CsvConstraintViolationException {
        Object o;
        try {
            o = convert(value);
        } catch (CsvRequiredFieldEmptyException e) {
            throw new CsvRequiredFieldEmptyException(bean.getClass(), field, e.getLocalizedMessage());
        }
        assignValueToField(bean, o);
    }

    /**
     * Assigns the given object to this field of the destination bean.
     * Uses a custom setter method if available.
     *
     * @param <T>  Type of the bean
     * @param bean The bean in which the field is located
     * @param obj  The data to be assigned to this field of the destination bean
     * @throws CsvDataTypeMismatchException If the data to be assigned cannot
     *                                      be converted to the type of the destination field
     */
    private <T> void assignValueToField(T bean, Object obj) throws CsvDataTypeMismatchException {

        // obj == null means that the source field was empty. Then we simply
        // leave the field as it was initialized by the VM. For primitives,
        // that will be values like 0, and for objects it will be null.
        if (obj != null) {
            Class<?> fieldType = field.getType();

            // Find and use a setter method if one is available.
            String setterName = "set" + Character.toUpperCase(field.getName().charAt(0)) + field.getName().substring(1);
            try {
                Method setterMethod = bean.getClass().getMethod(setterName, fieldType);
                try {
                    setterMethod.invoke(bean, obj);
                } catch (IllegalAccessException e) {
                    // Can't happen, because we've already established that the
                    // method is public through the use of getMethod().
                } catch (InvocationTargetException e) {
                    CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(obj, fieldType, e.getLocalizedMessage());
                    csve.initCause(e);
                    throw csve;
                }
            } catch (NoSuchMethodException e1) {

                // Otherwise set the field directly.
                try {
                    FieldUtils.writeField(field, bean, obj, true);
                } catch (IllegalAccessException e2) {
                    // Can only happen if the field is declared final.
                    // I'll take the risk.
                } catch (IllegalArgumentException e2) {
                    CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(obj, fieldType);
                    csve.initCause(e2);
                    throw csve;
                }
            } catch (SecurityException e1) {

                // Otherwise set the field directly.
                try {
                    FieldUtils.writeField(field, bean, obj, true);
                } catch (IllegalAccessException e2) {
                    // Can only happen if the field is declared final.
                    // I'll take the risk.
                } catch (IllegalArgumentException e2) {
                    CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(obj, fieldType);
                    csve.initCause(e2);
                    throw csve;
                }
            }
        }
    }

    /**
     * Method for converting from a string to the proper datatype of the
     * destination field.
     * This method must be specified in all non-abstract derived classes.
     *
     * @param value The string from the selected field of the CSV file
     * @return An Object representing the input data converted into the proper
     * type
     * @throws CsvDataTypeMismatchException    If the input string cannot be converted into
     *                                         the proper type
     * @throws CsvRequiredFieldEmptyException  If the field is mandatory but the input is
     *                                         empty
     * @throws CsvConstraintViolationException When the internal structure of
     *                                         data would be violated by the data in the CSV file
     */
    protected abstract Object convert(String value) throws CsvDataTypeMismatchException, CsvRequiredFieldEmptyException, CsvConstraintViolationException;
}
