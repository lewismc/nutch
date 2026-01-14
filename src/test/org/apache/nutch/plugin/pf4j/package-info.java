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
 * Tests for the PF4J (Plugin Framework for Java) integration in Nutch.
 * 
 * <p>This package contains unit and integration tests for the PF4J adapter layer
 * that enables Nutch to use modern annotation-based plugin discovery.</p>
 * 
 * <h2>Test Classes</h2>
 * <ul>
 *   <li>{@link org.apache.nutch.plugin.pf4j.TestNutchPluginStatusProvider} - 
 *       Tests plugin include/exclude pattern filtering</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.TestNutchExtensionFactory} - 
 *       Tests Configuration injection into extensions</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.TestNutchPluginManager} - 
 *       Tests the custom PF4J plugin manager for Nutch</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.TestExtensionHelper} - 
 *       Tests extension retrieval and ordering utilities</li>
 * </ul>
 * 
 * <h2>Test Fixtures</h2>
 * <ul>
 *   <li>{@link org.apache.nutch.plugin.pf4j.ITestPF4JExtension} - 
 *       Test extension point interface</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.TestPF4JExtensionImpl} - 
 *       {@code @Extension} annotated Configurable implementation</li>
 *   <li>{@link org.apache.nutch.plugin.pf4j.SimpleTestExtension} - 
 *       {@code @Extension} annotated non-Configurable implementation</li>
 * </ul>
 * 
 * @see org.apache.nutch.plugin.pf4j
 * @see org.apache.nutch.plugin.TestPluginSystem
 */
package org.apache.nutch.plugin.pf4j;
