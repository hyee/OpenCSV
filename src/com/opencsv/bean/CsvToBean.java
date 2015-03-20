package com.opencsv.bean;

/**
 Copyright 2007 Kyle Miller.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import com.opencsv.CSVReader;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts CSV data to objects.
 *
 * @param <T> - class to convert the objects to.
 */
public class CsvToBean<T> {
   private Map<Class<?>, PropertyEditor> editorMap = null;

   /**
    * Default constructor.
    */
   public CsvToBean() {
   }

   /**
    * parse the values from a csvReader constructed from the passed in Reader.
    * @param mapper - mapping strategy for the bean.
    * @param reader - Reader used to construct a CSVReader
    * @return List of Objects.
    */

   public List<T> parse(MappingStrategy<T> mapper, Reader reader) {
      return parse(mapper, new CSVReader(reader));
   }

   /**
    * parse the values from a csvReader constructed from the passed in Reader.
    * @param mapper - mapping strategy for the bean.
    * @param reader - Reader used to construct a CSVReader
    * @param filter - CsvToBeanFilter to apply - null if no filter.
    * @return List of Objects.
    */
   public List<T> parse(MappingStrategy<T> mapper, Reader reader, CsvToBeanFilter filter) {
      return parse(mapper, new CSVReader(reader), filter);
   }

   /**
    * parse the values from the csvReader.
    * @param mapper - mapping strategy for the bean.
    * @param csv - CSVReader
    * @return List of Objects.
    */
   public List<T> parse(MappingStrategy<T> mapper, CSVReader csv) {
      return parse(mapper, csv, null);
   }

   /**
    * parse the values from the csvReader.
    * @param mapper - mapping strategy for the bean.
    * @param csv - CSVReader
    * @param filter - CsvToBeanFilter to apply - null if no filter.
    * @return List of Objects.
    */
   public List<T> parse(MappingStrategy<T> mapper, CSVReader csv, CsvToBeanFilter filter) {
      try {
         mapper.captureHeader(csv);
         String[] line;
         List<T> list = new ArrayList<T>();
         while (null != (line = csv.readNext())) {
            processLine(mapper, filter, line, list);
         }
         return list;
      } catch (Exception e) {
         throw new RuntimeException("Error parsing CSV!", e);
      }
   }

   private void processLine(MappingStrategy<T> mapper, CsvToBeanFilter filter, String[] line, List<T> list) throws IllegalAccessException, InvocationTargetException, InstantiationException, IntrospectionException {
      if (filter == null || filter.allowLine(line)) {
         T obj = processLine(mapper, line);
         list.add(obj);
      }
   }

   /**
    * Creates a single object from a line from the csv file.
    * @param mapper - MappingStrategy
    * @param line  - array of Strings from the csv file.
    * @return - object containing the values.
    * @throws IllegalAccessException - thrown on error creating bean.
    * @throws InvocationTargetException - thrown on error calling the setters.
    * @throws InstantiationException - thrown on error creating bean.
    * @throws IntrospectionException - thrown on error getting the PropertyDescriptor.
    */
   protected T processLine(MappingStrategy<T> mapper, String[] line) throws IllegalAccessException, InvocationTargetException, InstantiationException, IntrospectionException {
      T bean = mapper.createBean();
      for (int col = 0; col < line.length; col++) {
         PropertyDescriptor prop = mapper.findDescriptor(col);
         if (null != prop) {
            String value = checkForTrim(line[col], prop);
            Object obj = convertValue(value, prop);
            prop.getWriteMethod().invoke(bean, obj);
         }
      }
      return bean;
   }

   private String checkForTrim(String s, PropertyDescriptor prop) {
      return trimmableProperty(prop) ? s.trim() : s;
   }

   private boolean trimmableProperty(PropertyDescriptor prop) {
      return !prop.getPropertyType().getName().contains("String");
   }

   /**
    * Convert a string value to its Object value.
    *
    * @param value - String value
    * @param prop  - PropertyDescriptor
    * @return The object set to value (i.e. Integer).  Will return String if no PropertyEditor is found.
    * @throws InstantiationException - Thrown on error getting the property editor from the property descriptor.
    * @throws IllegalAccessException - Thrown on error getting the property editor from the property descriptor.
    */
   protected Object convertValue(String value, PropertyDescriptor prop) throws InstantiationException, IllegalAccessException {
      PropertyEditor editor = getPropertyEditor(prop);
      Object obj = value;
      if (null != editor) {
         editor.setAsText(value);
         obj = editor.getValue();
      }
      return obj;
   }

   private PropertyEditor getPropertyEditorValue(Class<?> cls) {
      if (editorMap == null) {
         editorMap = new HashMap();
      }

      PropertyEditor editor = editorMap.get(cls);

      if (editor == null) {
         editor = PropertyEditorManager.findEditor(cls);
         addEditorToMap(cls, editor);
      }

      return editor;
   }

   private void addEditorToMap(Class<?> cls, PropertyEditor editor) {
      if (editor != null) {
         editorMap.put(cls, editor);
      }
   }


   /**
    * Attempt to find custom property editor on descriptor first, else try the propery editor manager.
    *
    * @param desc - PropertyDescriptor.
    * @return - the PropertyEditor for the given PropertyDescriptor.
    * @throws InstantiationException - thrown when getting the PropertyEditor for the class.
    * @throws IllegalAccessException - thrown when getting the PropertyEditor for the class.
    */
   protected PropertyEditor getPropertyEditor(PropertyDescriptor desc) throws InstantiationException, IllegalAccessException {
      Class<?> cls = desc.getPropertyEditorClass();
      if (null != cls) {
         return (PropertyEditor) cls.newInstance();
      }
      return getPropertyEditorValue(desc.getPropertyType());
   }
}
