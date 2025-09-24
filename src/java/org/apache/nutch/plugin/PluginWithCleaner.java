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
import java.lang.ref.Cleaner;

/**
 * A modern replacement for the Plugin class that uses the Cleaner API
 * (Java 9+) to handle resource cleanup without the deprecated finalize() method.
 * 
 * The Cleaner API provides a more reliable and performant alternative to
 * finalization for automatic resource management.
 * 
 * @author Apache Nutch Team
 * @since Nutch 1.20
 */
public class PluginWithCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Cleaner cleaner = Cleaner.create();
  
  private PluginDescriptor fDescriptor;
  protected Configuration conf;
  private final Cleaner.Cleanable cleanable;
  private final CleanupState state;

  /**
   * State object that holds the resources to be cleaned up.
   * This class must not hold references to the outer Plugin instance.
   */
  private static class CleanupState implements Runnable {
    private final String pluginId;
    private volatile boolean cleaned = false;

    CleanupState(String pluginId) {
      this.pluginId = pluginId;
    }

    @Override
    public void run() {
      if (!cleaned) {
        try {
          // Perform cleanup operations here
          // Note: Cannot access outer class instance fields
          LOG.debug("Cleaning up plugin: {}", pluginId);
          performCleanup();
        } finally {
          cleaned = true;
        }
      }
    }

    private void performCleanup() {
      // Implementation-specific cleanup logic
      // This should match what was in the original shutDown() method
    }
  }

  /**
   * Overloaded constructor
   * @param pDescriptor a plugin descriptor
   * @param conf a populated {@link org.apache.hadoop.conf.Configuration}
   */
  public PluginWithCleaner(PluginDescriptor pDescriptor, Configuration conf) {
    setDescriptor(pDescriptor);
    this.conf = conf;
    this.state = new CleanupState(pDescriptor.getPluginId());
    this.cleanable = cleaner.register(this, state);
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
   * This method can be called explicitly for immediate cleanup.
   * 
   * @throws PluginRuntimeException
   *           if a problems occurs until shutdown the plugin.
   */
  public void shutDown() throws PluginRuntimeException {
    // Trigger the cleanup immediately
    cleanable.clean();
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
   * Explicitly close and clean up the plugin resources.
   * This method provides a way to manually trigger cleanup.
   */
  public void close() {
    try {
      shutDown();
    } catch (PluginRuntimeException e) {
      LOG.error("Error during plugin shutdown: ", e);
    }
  }
}