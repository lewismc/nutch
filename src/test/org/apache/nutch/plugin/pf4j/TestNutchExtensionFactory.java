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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NutchExtensionFactory}.
 * 
 * <p>Tests that Configuration is properly injected into extensions
 * that implement {@link Configurable}.</p>
 */
public class TestNutchExtensionFactory {

  private Configuration conf;
  private PluginManager pluginManager;
  private NutchExtensionFactory factory;

  @BeforeEach
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.set("test.property", "test-value");
    pluginManager = new DefaultPluginManager();
    factory = new NutchExtensionFactory(pluginManager, conf);
  }

  /**
   * Test that Configuration is injected into Configurable extensions.
   */
  @Test
  public void testConfigurationInjected() {
    // Create an extension that implements Configurable
    TestPF4JExtensionImpl extension = factory.create(TestPF4JExtensionImpl.class);
    
    assertNotNull(extension, "Extension should be created");
    assertNotNull(extension.getConfiguration(), "Configuration should be injected");
    assertEquals(conf, extension.getConf(), "Should be the same Configuration instance");
    assertEquals("test-value", extension.getConfiguration().get("test.property"),
        "Configuration property should be accessible");
  }

  /**
   * Test that extensions without Configurable work correctly.
   */
  @Test
  public void testNonConfigurableExtension() {
    // Create an extension that does NOT implement Configurable
    SimpleTestExtension extension = factory.create(SimpleTestExtension.class);
    
    assertNotNull(extension, "Extension should be created");
    assertNull(extension.getConfiguration(), 
        "Non-configurable extension should not have Configuration");
    
    // Extension should still function
    assertEquals("simple: test", extension.process("test"));
  }

  /**
   * Test that the factory stores the Configuration correctly.
   */
  @Test
  public void testGetConfiguration() {
    assertSame(conf, factory.getConfiguration(), 
        "Factory should return the same Configuration");
  }

  /**
   * Test that the factory handles null gracefully.
   */
  @Test
  public void testCreateWithNullConfiguration() {
    NutchExtensionFactory nullConfFactory = new NutchExtensionFactory(pluginManager, null);
    
    // Creating extension should not throw
    TestPF4JExtensionImpl extension = nullConfFactory.create(TestPF4JExtensionImpl.class);
    
    assertNotNull(extension, "Extension should be created");
    assertNull(extension.getConfiguration(), "Configuration should be null");
  }

  /**
   * Test that extension functionality works after Configuration injection.
   */
  @Test
  public void testExtensionFunctionalityAfterInjection() {
    TestPF4JExtensionImpl extension = factory.create(TestPF4JExtensionImpl.class);
    
    // Test that the extension works
    assertEquals("hello processed", extension.process("hello"));
    assertEquals("null", extension.process(null));
  }

  /**
   * Test creating multiple extensions share the same Configuration.
   */
  @Test
  public void testMultipleExtensionsShareConfiguration() {
    TestPF4JExtensionImpl ext1 = factory.create(TestPF4JExtensionImpl.class);
    TestPF4JExtensionImpl ext2 = factory.create(TestPF4JExtensionImpl.class);
    
    assertNotNull(ext1.getConfiguration());
    assertNotNull(ext2.getConfiguration());
    
    // Both should have the same Configuration instance
    assertSame(ext1.getConfiguration(), ext2.getConfiguration(),
        "All extensions should share the same Configuration");
  }

  /**
   * Test extension creation with different Configuration values.
   */
  @Test
  public void testConfigurationValues() {
    // Set some Nutch-specific properties
    conf.set("http.agent.name", "TestBot");
    conf.setInt("http.timeout", 30000);
    conf.setBoolean("parser.html.form.use_action", true);
    
    TestPF4JExtensionImpl extension = factory.create(TestPF4JExtensionImpl.class);
    Configuration extConf = extension.getConfiguration();
    
    assertEquals("TestBot", extConf.get("http.agent.name"));
    assertEquals(30000, extConf.getInt("http.timeout", 0));
    assertTrue(extConf.getBoolean("parser.html.form.use_action", false));
  }
}
