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
import java.util.regex.Pattern;

import org.pf4j.PluginStatusProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin status provider that uses Nutch's include/exclude regex patterns.
 * 
 * <p>This provider implements the filtering logic from the legacy Nutch plugin system,
 * using the <code>plugin.includes</code> and <code>plugin.excludes</code> configuration
 * properties to determine which plugins should be enabled.</p>
 * 
 * @see org.pf4j.PluginStatusProvider
 */
public class NutchPluginStatusProvider implements PluginStatusProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Pattern includePattern;
  private final Pattern excludePattern;

  /**
   * Creates a new NutchPluginStatusProvider.
   * 
   * @param includePattern regex pattern for plugins to include (null means include all)
   * @param excludePattern regex pattern for plugins to exclude (null means exclude none)
   */
  public NutchPluginStatusProvider(Pattern includePattern, Pattern excludePattern) {
    this.includePattern = includePattern;
    this.excludePattern = excludePattern;
  }

  @Override
  public boolean isPluginDisabled(String pluginId) {
    // Check excludes first - if it matches exclude pattern, it's disabled
    if (excludePattern != null && excludePattern.matcher(pluginId).matches()) {
      LOG.debug("Plugin '{}' is disabled (matches exclude pattern)", pluginId);
      return true;
    }
    
    // If no include pattern specified, plugin is enabled (not disabled)
    if (includePattern == null) {
      return false;
    }
    
    // If include pattern exists, plugin is disabled if it doesn't match
    boolean matches = includePattern.matcher(pluginId).matches();
    if (!matches) {
      LOG.debug("Plugin '{}' is disabled (does not match include pattern)", pluginId);
    }
    return !matches;
  }

  @Override
  public void disablePlugin(String pluginId) {
    // This implementation uses static patterns from configuration
    // Dynamic enable/disable is not supported
    LOG.warn("Dynamic plugin disable not supported. Plugin '{}' status is determined by configuration patterns.", pluginId);
  }

  @Override
  public void enablePlugin(String pluginId) {
    // This implementation uses static patterns from configuration
    // Dynamic enable/disable is not supported
    LOG.warn("Dynamic plugin enable not supported. Plugin '{}' status is determined by configuration patterns.", pluginId);
  }
}
