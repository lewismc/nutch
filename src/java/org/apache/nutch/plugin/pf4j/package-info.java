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

/**
 * PF4J integration for the Nutch plugin system.
 * 
 * <p>This package provides the integration layer between Nutch and the
 * <a href="https://pf4j.org/">Plugin Framework for Java (PF4J)</a>.</p>
 * 
 * <h2>Key Components</h2>
 * <ul>
 *   <li>{@link org.apache.nutch.plugin.pf4j.NutchPluginManager} - Custom PF4J plugin manager
 *       that integrates with Nutch's Hadoop Configuration and plugin filtering</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.NutchExtensionFactory} - Factory that injects
 *       Hadoop Configuration into extensions implementing {@link org.apache.hadoop.conf.Configurable}</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.NutchPlugin} - Base class for plugins needing
 *       lifecycle management (startUp/shutDown)</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.NutchExtensionPoint} - Marker interface for
 *       Nutch extension points (extends PF4J's ExtensionPoint)</li>
 * </ul>
 * 
 * <h2>Migration from Legacy Plugin System</h2>
 * <p>This package is part of Nutch's migration from its custom XML-based plugin system
 * to PF4J. The migration supports both systems during the transition period:</p>
 * <ul>
 *   <li>Legacy plugins use <code>plugin.xml</code> manifest files</li>
 *   <li>PF4J plugins use <code>plugin.properties</code> and <code>@Extension</code> annotations</li>
 * </ul>
 * 
 * <h2>Creating a PF4J Plugin</h2>
 * <ol>
 *   <li>Create a <code>plugin.properties</code> file with plugin metadata</li>
 *   <li>Annotate extension classes with <code>@Extension</code></li>
 *   <li>Implement the appropriate extension point interface</li>
 * </ol>
 * 
 * <h3>Example plugin.properties:</h3>
 * <pre>
 * plugin.id=my-protocol
 * plugin.version=1.0.0
 * plugin.class=org.example.MyProtocolPlugin
 * plugin.provider=Example Corp
 * plugin.dependencies=lib-http
 * </pre>
 * 
 * <h3>Example Extension:</h3>
 * <pre>
 * &#64;Extension
 * public class MyProtocol implements Protocol {
 *     private Configuration conf;
 *     
 *     &#64;Override
 *     public void setConf(Configuration conf) {
 *         this.conf = conf;
 *     }
 *     
 *     &#64;Override
 *     public Configuration getConf() {
 *         return conf;
 *     }
 *     
 *     // ... implement Protocol methods
 * }
 * </pre>
 * 
 * @see <a href="https://pf4j.org/">PF4J Documentation</a>
 * @see org.apache.nutch.plugin.PluginRepository
 */
package org.apache.nutch.plugin.pf4j;
