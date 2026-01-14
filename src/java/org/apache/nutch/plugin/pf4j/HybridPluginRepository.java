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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.NutchConfiguration;
import org.pf4j.PluginManager;
import org.pf4j.PluginState;
import org.pf4j.PluginWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hybrid plugin repository that supports both legacy XML-based plugins and PF4J plugins.
 * 
 * <p>This class provides a unified interface for discovering and loading extensions from both:
 * <ul>
 *   <li>Legacy Nutch plugins using <code>plugin.xml</code> manifests</li>
 *   <li>PF4J plugins using <code>plugin.properties</code> and <code>@Extension</code> annotations</li>
 * </ul>
 * </p>
 * 
 * <p>During the migration period, this class allows both plugin systems to coexist.
 * Extensions from both systems are merged and presented through a unified API.</p>
 * 
 * <p>Usage:
 * <pre>
 * Configuration conf = NutchConfiguration.create();
 * HybridPluginRepository repo = HybridPluginRepository.get(conf);
 * 
 * // Get extensions from both legacy and PF4J plugins
 * List&lt;Protocol&gt; protocols = repo.getExtensions(Protocol.class);
 * </pre>
 * </p>
 */
public class HybridPluginRepository {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final WeakHashMap<String, HybridPluginRepository> CACHE = new WeakHashMap<>();

  private final Configuration conf;
  private final PluginRepository legacyRepository;
  private final NutchPluginManager pf4jManager;
  private final Map<Class<?>, List<?>> extensionCache = new HashMap<>();

  /**
   * Creates a new HybridPluginRepository.
   * 
   * @param conf the Hadoop Configuration
   */
  public HybridPluginRepository(Configuration conf) {
    this.conf = conf;
    
    // Initialize legacy repository
    this.legacyRepository = PluginRepository.get(conf);
    LOG.info("Initialized legacy PluginRepository");
    
    // Initialize PF4J manager
    this.pf4jManager = new NutchPluginManager(conf);
    
    // Load and start PF4J plugins
    pf4jManager.loadPlugins();
    pf4jManager.startPlugins();
    
    int pf4jPluginCount = pf4jManager.getStartedPlugins().size();
    LOG.info("Loaded {} PF4J plugins", pf4jPluginCount);
    
    for (PluginWrapper plugin : pf4jManager.getStartedPlugins()) {
      LOG.debug("PF4J plugin loaded: {} v{}", plugin.getPluginId(), plugin.getDescriptor().getVersion());
    }
  }

  /**
   * Gets a cached instance of the HybridPluginRepository.
   * 
   * @param conf the Hadoop Configuration
   * @return a cached repository instance
   */
  public static synchronized HybridPluginRepository get(Configuration conf) {
    String uuid = NutchConfiguration.getUUID(conf);
    if (uuid == null) {
      uuid = "nonNutchConf@" + conf.hashCode();
    }
    
    HybridPluginRepository result = CACHE.get(uuid);
    if (result == null) {
      result = new HybridPluginRepository(conf);
      CACHE.put(uuid, result);
    }
    return result;
  }

  /**
   * Gets all extensions of the specified type from both legacy and PF4J plugins.
   * 
   * <p>Extensions are merged from both plugin systems. PF4J extensions are discovered
   * first, followed by legacy extensions. The Hadoop Configuration is injected into
   * all extensions implementing {@link Configurable}.</p>
   * 
   * @param <T> the extension type
   * @param type the extension point interface class
   * @return list of extensions implementing the type
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> getExtensions(Class<T> type) {
    // Check cache first
    if (extensionCache.containsKey(type)) {
      return (List<T>) extensionCache.get(type);
    }
    
    List<T> allExtensions = new ArrayList<>();
    
    // Get PF4J extensions
    try {
      List<T> pf4jExtensions = pf4jManager.getExtensions(type);
      LOG.debug("Found {} PF4J extensions for {}", pf4jExtensions.size(), type.getName());
      
      for (T ext : pf4jExtensions) {
        // Configuration should already be injected by NutchExtensionFactory
        // but verify and inject if needed
        if (ext instanceof Configurable && ((Configurable) ext).getConf() == null) {
          ((Configurable) ext).setConf(conf);
        }
        allExtensions.add(ext);
      }
    } catch (Exception e) {
      LOG.warn("Error getting PF4J extensions for {}: {}", type.getName(), e.getMessage());
    }
    
    // Get legacy extensions
    try {
      String xPointId = getExtensionPointId(type);
      if (xPointId != null) {
        ExtensionPoint point = legacyRepository.getExtensionPoint(xPointId);
        if (point != null) {
          Extension[] extensions = point.getExtensions();
          LOG.debug("Found {} legacy extensions for {}", extensions.length, type.getName());
          
          for (Extension ext : extensions) {
            try {
              Object instance = ext.getExtensionInstance();
              if (type.isInstance(instance)) {
                // Check if this extension was already added from PF4J
                // (avoid duplicates during migration)
                boolean isDuplicate = false;
                for (T existing : allExtensions) {
                  if (existing.getClass().equals(instance.getClass())) {
                    isDuplicate = true;
                    break;
                  }
                }
                
                if (!isDuplicate) {
                  allExtensions.add(type.cast(instance));
                }
              }
            } catch (PluginRuntimeException e) {
              LOG.warn("Error instantiating legacy extension {}: {}", ext.getId(), e.getMessage());
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Error getting legacy extensions for {}: {}", type.getName(), e.getMessage());
    }
    
    LOG.info("Total {} extensions found for {}", allExtensions.size(), type.getSimpleName());
    
    // Cache the result
    extensionCache.put(type, allExtensions);
    
    return allExtensions;
  }

  /**
   * Gets the extension point ID for a given type.
   * 
   * <p>By convention, Nutch extension point IDs are the fully qualified class name
   * of the extension point interface.</p>
   */
  private String getExtensionPointId(Class<?> type) {
    // Try to get X_POINT_ID field (Nutch convention)
    try {
      return (String) type.getField("X_POINT_ID").get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      // Fall back to class name
      return type.getName();
    }
  }

  /**
   * Gets the legacy PluginRepository.
   * 
   * @return the legacy repository
   */
  public PluginRepository getLegacyRepository() {
    return legacyRepository;
  }

  /**
   * Gets the PF4J PluginManager.
   * 
   * @return the PF4J plugin manager
   */
  public NutchPluginManager getPf4jManager() {
    return pf4jManager;
  }

  /**
   * Gets the Hadoop Configuration.
   * 
   * @return the configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Shuts down the repository and all plugins.
   */
  public void shutdown() {
    LOG.info("Shutting down HybridPluginRepository");
    
    // Stop PF4J plugins
    pf4jManager.stopPlugins();
    
    // Clear cache
    extensionCache.clear();
  }
}
