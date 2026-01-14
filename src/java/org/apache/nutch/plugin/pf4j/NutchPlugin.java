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

import org.apache.hadoop.conf.Configuration;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Nutch plugins using PF4J.
 * 
 * <p>This class provides a bridge between PF4J's plugin lifecycle and Nutch's
 * traditional plugin model. It provides access to the Hadoop Configuration
 * and maps PF4J lifecycle methods to Nutch's startUp/shutDown pattern.</p>
 * 
 * <p>Plugin developers extending this class can override:
 * <ul>
 *   <li>{@link #startUp()} - Called when the plugin is started</li>
 *   <li>{@link #shutDown()} - Called when the plugin is stopped</li>
 * </ul>
 * </p>
 * 
 * <p>Note: Most simple plugins that only provide extensions do not need to
 * extend this class. This is only needed for plugins that require lifecycle
 * management (e.g., database connections, resource initialization).</p>
 * 
 * @see org.pf4j.Plugin
 * @see org.apache.nutch.plugin.Plugin
 */
public class NutchPlugin extends Plugin {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected Configuration conf;

  /**
   * Creates a new NutchPlugin.
   * 
   * @param wrapper the PF4J plugin wrapper
   */
  public NutchPlugin(PluginWrapper wrapper) {
    super(wrapper);
    
    // Get Configuration from the NutchPluginManager if available
    if (wrapper.getPluginManager() instanceof NutchPluginManager) {
      this.conf = ((NutchPluginManager) wrapper.getPluginManager()).getConfiguration();
    }
  }

  @Override
  public void start() {
    LOG.info("Starting plugin: {}", wrapper.getPluginId());
    try {
      startUp();
    } catch (Exception e) {
      LOG.error("Failed to start plugin: {}", wrapper.getPluginId(), e);
      throw new RuntimeException("Plugin startup failed: " + wrapper.getPluginId(), e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping plugin: {}", wrapper.getPluginId());
    try {
      shutDown();
    } catch (Exception e) {
      LOG.error("Failed to stop plugin: {}", wrapper.getPluginId(), e);
      throw new RuntimeException("Plugin shutdown failed: " + wrapper.getPluginId(), e);
    }
  }

  /**
   * Called when the plugin is started.
   * 
   * <p>Override this method to perform plugin-specific initialization,
   * such as opening database connections or initializing resources.</p>
   * 
   * @throws Exception if startup fails
   */
  protected void startUp() throws Exception {
    // Default implementation does nothing
  }

  /**
   * Called when the plugin is stopped.
   * 
   * <p>Override this method to perform plugin-specific cleanup,
   * such as closing database connections or releasing resources.</p>
   * 
   * @throws Exception if shutdown fails
   */
  protected void shutDown() throws Exception {
    // Default implementation does nothing
  }

  /**
   * Gets the Hadoop Configuration for this plugin.
   * 
   * @return the Configuration, or null if not available
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Sets the Hadoop Configuration for this plugin.
   * 
   * @param conf the Configuration to set
   */
  public void setConfiguration(Configuration conf) {
    this.conf = conf;
  }
}
