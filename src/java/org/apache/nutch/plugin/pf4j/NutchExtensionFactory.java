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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.pf4j.DefaultExtensionFactory;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension factory that injects Hadoop Configuration into Nutch extensions.
 * 
 * <p>When an extension implements {@link Configurable}, this factory automatically
 * calls {@link Configurable#setConf(Configuration)} after instantiation, mirroring
 * the behavior of the legacy Nutch plugin system.</p>
 * 
 * @see org.pf4j.ExtensionFactory
 */
public class NutchExtensionFactory extends DefaultExtensionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final PluginManager pluginManager;
  private final Configuration conf;

  /**
   * Creates a new NutchExtensionFactory.
   * 
   * @param pluginManager the PF4J plugin manager
   * @param conf the Hadoop Configuration to inject into extensions
   */
  public NutchExtensionFactory(PluginManager pluginManager, Configuration conf) {
    this.pluginManager = pluginManager;
    this.conf = conf;
  }

  @Override
  public <T> T create(Class<T> extensionClass) {
    T extension = super.create(extensionClass);
    
    if (extension != null) {
      // Inject Hadoop Configuration if the extension is Configurable
      if (extension instanceof Configurable) {
        LOG.debug("Injecting Configuration into extension: {}", extensionClass.getName());
        ((Configurable) extension).setConf(conf);
      }
    }
    
    return extension;
  }

  /**
   * Gets the Hadoop Configuration used by this factory.
   * 
   * @return the Configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }
}
