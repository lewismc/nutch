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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ExtensionFactory;
import org.pf4j.PluginDescriptorFinder;
import org.pf4j.PluginLoader;
import org.pf4j.PluginStatusProvider;
import org.pf4j.PropertiesPluginDescriptorFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom PF4J PluginManager that integrates with Nutch's Hadoop Configuration.
 * 
 * <p>This manager:
 * <ul>
 *   <li>Reads plugin directories from Nutch's <code>plugin.folders</code> configuration</li>
 *   <li>Applies <code>plugin.includes</code> and <code>plugin.excludes</code> filters</li>
 *   <li>Injects Hadoop Configuration into extensions via {@link NutchExtensionFactory}</li>
 * </ul>
 * </p>
 * 
 * @see org.pf4j.DefaultPluginManager
 */
public class NutchPluginManager extends DefaultPluginManager {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Configuration conf;
  private final Pattern includePattern;
  private final Pattern excludePattern;

  /**
   * Creates a NutchPluginManager using configuration from the provided Hadoop Configuration.
   * 
   * @param conf Hadoop Configuration containing plugin settings
   */
  public NutchPluginManager(Configuration conf) {
    super(getPluginPaths(conf));
    this.conf = conf;
    
    String includes = conf.get("plugin.includes", "");
    String excludes = conf.get("plugin.excludes", "");
    
    this.includePattern = includes.isEmpty() ? null : Pattern.compile(includes);
    this.excludePattern = excludes.isEmpty() ? null : Pattern.compile(excludes);
    
    LOG.info("NutchPluginManager initialized with {} plugin paths", getPluginsRoots().size());
    if (includePattern != null) {
      LOG.info("Plugin includes pattern: {}", includes);
    }
    if (excludePattern != null) {
      LOG.info("Plugin excludes pattern: {}", excludes);
    }
  }

  /**
   * Creates a NutchPluginManager with explicit plugin paths.
   * 
   * @param conf Hadoop Configuration for extension injection
   * @param pluginPaths List of paths to plugin directories
   */
  public NutchPluginManager(Configuration conf, List<Path> pluginPaths) {
    super(pluginPaths);
    this.conf = conf;
    
    String includes = conf.get("plugin.includes", "");
    String excludes = conf.get("plugin.excludes", "");
    
    this.includePattern = includes.isEmpty() ? null : Pattern.compile(includes);
    this.excludePattern = excludes.isEmpty() ? null : Pattern.compile(excludes);
  }

  /**
   * Extracts plugin paths from Nutch configuration.
   */
  private static List<Path> getPluginPaths(Configuration conf) {
    List<Path> paths = new ArrayList<>();
    String[] pluginFolders = conf.getStrings("plugin.folders");
    
    if (pluginFolders != null) {
      for (String folder : pluginFolders) {
        Path path = Paths.get(folder);
        paths.add(path);
        LOG.debug("Adding plugin path: {}", path);
      }
    }
    
    if (paths.isEmpty()) {
      // Default to "plugins" directory
      paths.add(Paths.get("plugins"));
      LOG.warn("No plugin.folders configured, using default 'plugins' directory");
    }
    
    return paths;
  }

  @Override
  protected ExtensionFactory createExtensionFactory() {
    return new NutchExtensionFactory(this, conf);
  }

  @Override
  protected PluginDescriptorFinder createPluginDescriptorFinder() {
    // Support both plugin.properties (PF4J standard) and plugin.xml (legacy Nutch)
    return new PropertiesPluginDescriptorFinder();
  }

  @Override
  protected PluginStatusProvider createPluginStatusProvider() {
    return new NutchPluginStatusProvider(includePattern, excludePattern);
  }

  /**
   * Gets the Hadoop Configuration associated with this plugin manager.
   * 
   * @return the Hadoop Configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Checks if a plugin should be included based on the include/exclude patterns.
   * 
   * @param pluginId the plugin ID to check
   * @return true if the plugin should be included
   */
  public boolean isPluginIncluded(String pluginId) {
    // Check excludes first
    if (excludePattern != null && excludePattern.matcher(pluginId).matches()) {
      return false;
    }
    
    // If no include pattern, include all (that aren't excluded)
    if (includePattern == null) {
      return true;
    }
    
    // Check against include pattern
    return includePattern.matcher(pluginId).matches();
  }
}
