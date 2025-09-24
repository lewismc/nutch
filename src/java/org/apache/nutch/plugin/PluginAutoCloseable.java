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
package org.apache.nutch.plugin;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A modern replacement for the Plugin class that implements AutoCloseable
 * to handle resource cleanup without using the deprecated finalize() method.
 * 
 * This class provides the same functionality as Plugin but uses the
 * try-with-resources pattern for automatic resource management.
 * 
 * @author Apache Nutch Team
 * @since Nutch 1.20
 */
public class PluginAutoCloseable implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private PluginDescriptor fDescriptor;
  protected Configuration conf;
  private volatile boolean closed = false;

  /**
   * Overloaded constructor
   * @param pDescriptor a plugin descriptor
   * @param conf a populated {@link org.apache.hadoop.conf.Configuration}
   */
  public PluginAutoCloseable(PluginDescriptor pDescriptor, Configuration conf) {
    setDescriptor(pDescriptor);
    this.conf = conf;
  }

  /**
   * Will be invoked until plugin start up. Since the nutch-plugin system use
   * lazy loading the start up is invoked until the first time a extension is
   * used.
   * 
   * @throws PluginRuntimeException
   *           If the startup was without success.
   */
  public void startUp() throws PluginRuntimeException {
    // Implementation specific startup logic
  }

  /**
   * Shutdown the plugin. This happens until nutch will be stopped.
   * 
   * @throws PluginRuntimeException
   *           if a problems occurs until shutdown the plugin.
   */
  public void shutDown() throws PluginRuntimeException {
    // Implementation specific shutdown logic
  }

  /**
   * Returns the plugin descriptor
   * 
   * @return PluginDescriptor
   */
  public PluginDescriptor getDescriptor() {
    return fDescriptor;
  }

  /**
   * @param descriptor
   *          The descriptor to set
   */
  private void setDescriptor(PluginDescriptor descriptor) {
    fDescriptor = descriptor;
  }

  /**
   * Implements the AutoCloseable interface to provide automatic resource cleanup.
   * This method replaces the deprecated finalize() method.
   * 
   * When used with try-with-resources, this method will be called automatically
   * to clean up plugin resources.
   * 
   * @throws Exception if an error occurs during cleanup
   */
  @Override
  public void close() throws Exception {
    if (!closed) {
      try {
        shutDown();
      } catch (PluginRuntimeException e) {
        LOG.error("Error during plugin shutdown: ", e);
        throw e;
      } finally {
        closed = true;
      }
    }
  }

  /**
   * Check if the plugin has been closed
   * @return true if the plugin has been closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }
}