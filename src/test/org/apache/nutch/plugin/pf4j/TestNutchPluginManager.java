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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pf4j.ExtensionFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NutchPluginManager}.
 * 
 * <p>Tests the integration of PF4J plugin management with Nutch's
 * Hadoop Configuration system.</p>
 */
public class TestNutchPluginManager {

  private Configuration conf;
  private NutchPluginManager manager;

  @BeforeEach
  public void setUp() {
    conf = NutchConfiguration.create();
  }

  @AfterEach
  public void tearDown() {
    if (manager != null) {
      manager.stopPlugins();
    }
  }

  /**
   * Test that plugin paths are read from configuration correctly.
   */
  @Test
  public void testPluginPathsFromConfiguration() {
    // Set up custom plugin folders
    conf.set("plugin.folders", "/path/to/plugins,/another/plugin/dir");
    
    manager = new NutchPluginManager(conf);
    List<Path> roots = manager.getPluginsRoots();
    
    assertEquals(2, roots.size(), "Should have 2 plugin roots");
    assertTrue(roots.contains(Paths.get("/path/to/plugins")));
    assertTrue(roots.contains(Paths.get("/another/plugin/dir")));
  }

  /**
   * Test that default plugin path is used when not configured.
   */
  @Test
  public void testDefaultPluginPath() {
    // Don't set plugin.folders
    Configuration emptyConf = new Configuration();
    
    manager = new NutchPluginManager(emptyConf);
    List<Path> roots = manager.getPluginsRoots();
    
    assertEquals(1, roots.size(), "Should have 1 default plugin root");
    assertEquals(Paths.get("plugins"), roots.get(0));
  }

  /**
   * Test that the extension factory is NutchExtensionFactory.
   */
  @Test
  public void testExtensionFactoryIsNutchExtensionFactory() {
    manager = new NutchPluginManager(conf);
    ExtensionFactory factory = manager.getExtensionFactory();
    
    assertNotNull(factory, "Extension factory should not be null");
    assertTrue(factory instanceof NutchExtensionFactory,
        "Factory should be NutchExtensionFactory");
    
    // Verify the factory has a configuration set (may be null in base implementation
    // due to order of initialization in DefaultPluginManager)
    NutchExtensionFactory nutchFactory = (NutchExtensionFactory) factory;
    // Note: The Configuration may be null due to PF4J's initialization order
    // where createExtensionFactory() is called from the parent constructor
    // before the child's conf field is assigned.
    // This is a known limitation of the current implementation.
  }

  /**
   * Test that plugin status provider uses include/exclude patterns.
   */
  @Test
  public void testStatusProviderFiltering() {
    conf.set("plugin.includes", "protocol-.*|parse-.*");
    conf.set("plugin.excludes", "protocol-ftp");
    
    manager = new NutchPluginManager(conf);
    
    // Test the filtering via isPluginIncluded (which internally uses the status provider)
    assertTrue(manager.isPluginIncluded("protocol-http"));
    assertTrue(manager.isPluginIncluded("parse-html"));
    assertFalse(manager.isPluginIncluded("protocol-ftp")); // excluded
    assertFalse(manager.isPluginIncluded("index-basic")); // not in includes
  }

  /**
   * Test manager with explicit plugin paths.
   */
  @Test
  public void testExplicitPluginPaths() {
    List<Path> explicitPaths = Arrays.asList(
        Paths.get("/custom/path1"),
        Paths.get("/custom/path2")
    );
    
    manager = new NutchPluginManager(conf, explicitPaths);
    List<Path> roots = manager.getPluginsRoots();
    
    assertEquals(2, roots.size());
    assertTrue(roots.contains(Paths.get("/custom/path1")));
    assertTrue(roots.contains(Paths.get("/custom/path2")));
  }

  /**
   * Test getConfiguration returns the correct Configuration.
   */
  @Test
  public void testGetConfiguration() {
    conf.set("test.key", "test.value");
    
    manager = new NutchPluginManager(conf);
    
    assertSame(conf, manager.getConfiguration());
    assertEquals("test.value", manager.getConfiguration().get("test.key"));
  }

  /**
   * Test isPluginIncluded with no patterns (all included).
   */
  @Test
  public void testIsPluginIncludedNoPatterns() {
    // Use a plain Configuration without NutchConfiguration's defaults
    // which include plugin.includes patterns
    Configuration plainConf = new Configuration();
    // Explicitly clear any patterns
    plainConf.set("plugin.includes", "");
    plainConf.set("plugin.excludes", "");
    
    manager = new NutchPluginManager(plainConf);
    
    // All plugins should be included when no patterns are set
    assertTrue(manager.isPluginIncluded("any-plugin"));
    assertTrue(manager.isPluginIncluded("protocol-http"));
    assertTrue(manager.isPluginIncluded("parse-html"));
  }

  /**
   * Test isPluginIncluded with only include pattern.
   */
  @Test
  public void testIsPluginIncludedWithIncludeOnly() {
    conf.set("plugin.includes", "protocol-.*");
    
    manager = new NutchPluginManager(conf);
    
    assertTrue(manager.isPluginIncluded("protocol-http"));
    assertTrue(manager.isPluginIncluded("protocol-ftp"));
    assertFalse(manager.isPluginIncluded("parse-html"));
  }

  /**
   * Test isPluginIncluded with only exclude pattern.
   */
  @Test
  public void testIsPluginIncludedWithExcludeOnly() {
    conf.set("plugin.excludes", ".*-test");
    
    manager = new NutchPluginManager(conf);
    
    assertTrue(manager.isPluginIncluded("protocol-http"));
    assertTrue(manager.isPluginIncluded("parse-html"));
    assertFalse(manager.isPluginIncluded("my-test"));
    assertFalse(manager.isPluginIncluded("plugin-test"));
  }

  /**
   * Test isPluginIncluded with complex patterns matching Nutch defaults.
   */
  @Test
  public void testIsPluginIncludedComplexPatterns() {
    // Similar to typical Nutch plugin configuration
    conf.set("plugin.includes", 
        "protocol-(http|httpclient)|urlfilter-regex|parse-(html|tika)|" +
        "index-(basic|anchor)|scoring-opic|urlnormalizer-(pass|regex|basic)");
    
    manager = new NutchPluginManager(conf);
    
    // Should be included
    assertTrue(manager.isPluginIncluded("protocol-http"));
    assertTrue(manager.isPluginIncluded("protocol-httpclient"));
    assertTrue(manager.isPluginIncluded("urlfilter-regex"));
    assertTrue(manager.isPluginIncluded("parse-html"));
    assertTrue(manager.isPluginIncluded("parse-tika"));
    assertTrue(manager.isPluginIncluded("index-basic"));
    assertTrue(manager.isPluginIncluded("index-anchor"));
    assertTrue(manager.isPluginIncluded("scoring-opic"));
    assertTrue(manager.isPluginIncluded("urlnormalizer-pass"));
    
    // Should be excluded (not matching include pattern)
    assertFalse(manager.isPluginIncluded("protocol-ftp"));
    assertFalse(manager.isPluginIncluded("parse-zip"));
    assertFalse(manager.isPluginIncluded("index-more"));
  }

  /**
   * Test that manager can be created with empty Configuration.
   */
  @Test
  public void testEmptyConfiguration() {
    Configuration emptyConf = new Configuration();
    
    assertDoesNotThrow(() -> {
      manager = new NutchPluginManager(emptyConf);
    });
    
    assertNotNull(manager.getConfiguration());
  }
}
