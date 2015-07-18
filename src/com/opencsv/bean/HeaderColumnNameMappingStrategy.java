package com.opencsv.bean;

import com.opencsv.CSVReader;
import com.opencsv.util.StringUtils;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*
 * Copyright 2007 Kyle Miller.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Maps data to objects using the column names in the first row of the csv
 * file as reference.  This way the column order does not matter.
 *
 * @param <T>
 */

public class HeaderColumnNameMappingStrategy<T> implements MappingStrategy<T> {
    protected String[] header;
    protected Map<String, Integer> indexLookup = new HashMap<String, Integer>();
    protected Map<String, PropertyDescriptor> descriptorMap = null;
    protected Class<T> type;

    /**
     * Default constructor.
     */
    public HeaderColumnNameMappingStrategy() {
    }

    /**
     * Retrieves the header from the CSVReader.
     *
     * @param reader the CSVReader to use for header parsing
     * @throws IOException - thrown on error reading from the CSVReader.
     */
    public void captureHeader(CSVReader reader) throws IOException {
        header = reader.readNext();
    }

    /**
     * Creates an index map of column names to column position.
     *
     * @param values - array of header values.
     */
    protected void createIndexLookup(String[] values) {
        if (indexLookup.isEmpty()) {
            for (int i = 0; i < values.length; i++) {
                indexLookup.put(values[i], i);
            }
        }
    }

    /**
     * Resets index map of column names to column position.
     */
    protected void resetIndexMap() {
        indexLookup.clear();
    }

    /**
     * Gets the column index that corresponds to a specific colum name.
     * If the CSV file doesn't have a header row, this method will always return
     * null.
     *
     * @param name the column name
     * @return the column index, or null if the name doesn't exist
     */
    public Integer getColumnIndex(String name) {
        if (null == header) {
            throw new IllegalStateException("The header row hasn't been read yet.");
        }

        createIndexLookup(header);

        return indexLookup.get(name);
    }

    /**
     * Gets the property descriptor for a given column position.
     *
     * @param col the column to find the description for
     * @return - the property descriptor for the column position or null if one could not be found.
     * @throws IntrospectionException - thrown on error retrieving the property description.
     */
    public PropertyDescriptor findDescriptor(int col) throws IntrospectionException {
        String columnName = getColumnName(col);
        return (StringUtils.isNotBlank(columnName)) ? findDescriptor(columnName) : null;
    }

    /**
     * Get the column name for a given column position.
     *
     * @param col - column position.
     * @return - the column name or null if the position is larger than the header array or there is no headers defined.
     */
    public String getColumnName(int col) {
        return (null != header && col < header.length) ? header[col] : null;
    }

    /**
     * Find the property descriptor for a given column.
     *
     * @param name - column name to look up.
     * @return - the property descriptor for the column.
     * @throws IntrospectionException - thrown on error loading the property descriptors.
     */
    protected PropertyDescriptor findDescriptor(String name) throws IntrospectionException {
        if (null == descriptorMap) {
            descriptorMap = loadDescriptorMap(); //lazy load descriptors
        }
        return descriptorMap.get(name.toUpperCase().trim());
    }

    /**
     * Determines if the name of a property descriptor matches the column name.
     * Currently only used by unit tests.
     *
     * @param name - name of the column.
     * @param desc - property descriptor to check against
     * @return - true if the name matches the name in the property descriptor.
     */
    protected boolean matches(String name, PropertyDescriptor desc) {
        return desc.getName().equals(name.trim());
    }

    /**
     * builds a map of property descriptors for the Bean.
     *
     * @return - map of property descriptors
     * @throws IntrospectionException - thrown on error getting information about the bean.
     */
    protected Map<String, PropertyDescriptor> loadDescriptorMap() throws IntrospectionException {
        Map<String, PropertyDescriptor> map = new HashMap<String, PropertyDescriptor>();

        PropertyDescriptor[] descriptors;
        descriptors = loadDescriptors(getType());  //TODO refactor this class to use T instead of getType.
        for (PropertyDescriptor descriptor : descriptors) {
            map.put(descriptor.getName().toUpperCase().trim(), descriptor);
        }

        return map;
    }

    private PropertyDescriptor[] loadDescriptors(Class<T> cls) throws IntrospectionException {
        BeanInfo beanInfo = Introspector.getBeanInfo(cls);
        return beanInfo.getPropertyDescriptors();
    }

    /**
     * Creates an object to be mapped.
     *
     * @return an object of type T.
     * @throws InstantiationException - thrown on error creating object.
     * @throws IllegalAccessException - thrown on error creating object.
     */
    public T createBean() throws InstantiationException, IllegalAccessException {
        return type.newInstance();
    }

    /**
     * get the class type that the Strategy is mapping.
     * This method is deprecated as the user should use the Java 5 conventions.
     *
     * @return Class of the object that mapper will create.
     */
    @Deprecated
    public Class<T> getType() {
        return type;
    }

    /**
     * Sets the class type that is being mapped.
     * This method is deprecated as the user should use the Java 5 conventions.
     *
     * @param type Class type.
     */
    @Deprecated
    public void setType(Class<T> type) {
        this.type = type;
    }
}
