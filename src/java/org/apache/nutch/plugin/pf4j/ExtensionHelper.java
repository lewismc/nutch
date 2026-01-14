/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.plugin.pf4j;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for working with PF4J extensions in Nutch.
 * 
 * <p>This class provides utility methods to simplify extension discovery
 * and management during the migration from the legacy plugin system to PF4J.</p>
 */
public final class ExtensionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ExtensionHelper() {
    // Utility class - no instantiation
  }

  /**
   * Gets all extensions of the specified type, with optional ordering.
   * 
   * @param <T> the extension type
   * @param conf the Hadoop Configuration
   * @param type the extension point interface class
   * @param orderProperty optional configuration property specifying extension order
   * @return list of ordered extensions
   */
  public static <T> List<T> getOrderedExtensions(Configuration conf, Class<T> type, String orderProperty) {
    HybridPluginRepository repo = HybridPluginRepository.get(conf);
    List<T> extensions = repo.getExtensions(type);
    
    if (orderProperty != null && !orderProperty.isEmpty()) {
      String order = conf.get(orderProperty);
      if (order != null && !order.trim().isEmpty()) {
        String[] orderedClasses = order.trim().split("\\s+");
        extensions = sortExtensions(extensions, orderedClasses);
      }
    }
    
    return extensions;
  }

  /**
   * Gets all extensions of the specified type.
   * 
   * @param <T> the extension type
   * @param conf the Hadoop Configuration
   * @param type the extension point interface class
   * @return list of extensions
   */
  public static <T> List<T> getExtensions(Configuration conf, Class<T> type) {
    return HybridPluginRepository.get(conf).getExtensions(type);
  }

  /**
   * Gets a single extension by class name.
   * 
   * @param <T> the extension type
   * @param conf the Hadoop Configuration
   * @param type the extension point interface class
   * @param className the fully qualified class name of the extension
   * @return the extension instance, or null if not found
   */
  public static <T> T getExtensionByClassName(Configuration conf, Class<T> type, String className) {
    List<T> extensions = getExtensions(conf, type);
    for (T ext : extensions) {
      if (ext.getClass().getName().equals(className)) {
        return ext;
      }
    }
    LOG.warn("Extension not found: {} for type {}", className, type.getName());
    return null;
  }

  /**
   * Sorts extensions according to the specified order.
   */
  private static <T> List<T> sortExtensions(List<T> extensions, String[] orderedClasses) {
    List<T> sorted = new ArrayList<>();
    List<T> remaining = new ArrayList<>(extensions);
    
    // Add extensions in the specified order
    for (String className : orderedClasses) {
      for (T ext : remaining) {
        if (ext.getClass().getName().equals(className)) {
          sorted.add(ext);
          remaining.remove(ext);
          break;
        }
      }
    }
    
    // Add any remaining extensions not in the order list
    sorted.addAll(remaining);
    
    return sorted;
  }

  /**
   * Checks if PF4J plugins are available for the specified extension type.
   * 
   * @param <T> the extension type
   * @param conf the Hadoop Configuration
   * @param type the extension point interface class
   * @return true if PF4J plugins provide extensions of this type
   */
  public static <T> boolean hasPf4jExtensions(Configuration conf, Class<T> type) {
    HybridPluginRepository repo = HybridPluginRepository.get(conf);
    NutchPluginManager pm = repo.getPf4jManager();
    List<T> pf4jExtensions = pm.getExtensions(type);
    return !pf4jExtensions.isEmpty();
  }

  /**
   * Logs information about loaded extensions.
   * 
   * @param <T> the extension type
   * @param conf the Hadoop Configuration
   * @param type the extension point interface class
   */
  public static <T> void logExtensionInfo(Configuration conf, Class<T> type) {
    List<T> extensions = getExtensions(conf, type);
    LOG.info("Loaded {} extensions for {}:", extensions.size(), type.getSimpleName());
    for (T ext : extensions) {
      LOG.info("  - {}", ext.getClass().getName());
    }
  }
}
