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

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link ExtensionHelper}.
 * 
 * <p>Tests extension retrieval and ordering functionality using the
 * hybrid plugin repository.</p>
 */
public class TestExtensionHelper {

  private Configuration conf;

  @BeforeEach
  public void setUp() {
    conf = NutchConfiguration.create();
    // Enable all plugins for testing
    conf.set("plugin.includes", ".*");
  }

  /**
   * Test getting extensions by type returns non-null list.
   */
  @Test
  public void testGetExtensionsReturnsNonNullList() {
    List<Protocol> protocols = ExtensionHelper.getExtensions(conf, Protocol.class);
    
    assertNotNull(protocols, "Extensions list should not be null");
  }

  /**
   * Test getting extensions with null type throws appropriate exception.
   */
  @Test
  public void testGetExtensionsWithNullType() {
    assertThrows(NullPointerException.class, () -> {
      ExtensionHelper.getExtensions(conf, null);
    });
  }

  /**
   * Test getExtensionByClassName returns null for non-existent class.
   */
  @Test
  public void testGetExtensionByClassNameNotFound() {
    Protocol result = ExtensionHelper.getExtensionByClassName(
        conf, Protocol.class, "org.apache.nutch.NonExistentProtocol");
    
    assertNull(result, "Should return null for non-existent extension");
  }

  /**
   * Test getOrderedExtensions with null order property returns unordered list.
   */
  @Test
  public void testGetOrderedExtensionsWithNullOrderProperty() {
    List<IndexingFilter> filters = ExtensionHelper.getOrderedExtensions(
        conf, IndexingFilter.class, null);
    
    assertNotNull(filters, "Should return non-null list");
  }

  /**
   * Test getOrderedExtensions with empty order property returns unordered list.
   */
  @Test
  public void testGetOrderedExtensionsWithEmptyOrderProperty() {
    List<IndexingFilter> filters = ExtensionHelper.getOrderedExtensions(
        conf, IndexingFilter.class, "");
    
    assertNotNull(filters, "Should return non-null list");
  }

  /**
   * Test getOrderedExtensions with non-existent order property returns unordered list.
   */
  @Test
  public void testGetOrderedExtensionsWithNonExistentProperty() {
    List<Parser> parsers = ExtensionHelper.getOrderedExtensions(
        conf, Parser.class, "non.existent.property");
    
    assertNotNull(parsers, "Should return non-null list even with non-existent property");
  }

  /**
   * Test hasPf4jExtensions returns boolean without throwing.
   */
  @Test
  public void testHasPf4jExtensions() {
    assertDoesNotThrow(() -> {
      boolean hasPf4j = ExtensionHelper.hasPf4jExtensions(conf, Protocol.class);
      // Result can be true or false depending on plugin loading
    });
  }

  /**
   * Test logExtensionInfo does not throw.
   */
  @Test
  public void testLogExtensionInfo() {
    assertDoesNotThrow(() -> {
      ExtensionHelper.logExtensionInfo(conf, Protocol.class);
    });
  }

  /**
   * Test that loaded extensions have Configuration injected.
   */
  @Test
  public void testExtensionsHaveConfiguration() {
    // Set a unique property to verify configuration is passed
    conf.set("test.helper.property", "test-helper-value");
    
    List<IndexingFilter> filters = ExtensionHelper.getExtensions(conf, IndexingFilter.class);
    
    // Check that Configurable extensions have Configuration set
    for (IndexingFilter filter : filters) {
      if (filter instanceof Configurable) {
        Configuration filterConf = ((Configurable) filter).getConf();
        // The Configuration should be set (may be the same or a copy)
        assertNotNull(filterConf, 
            "Configurable extensions should have Configuration set");
      }
    }
  }

  /**
   * Test ordering of extensions when order property is set.
   */
  @Test
  public void testExtensionOrdering() {
    // Configure ordering - list specific class names in order
    conf.set("test.extension.order", 
        "org.apache.nutch.indexer.basic.BasicIndexingFilter " +
        "org.apache.nutch.indexer.anchor.AnchorIndexingFilter");
    
    List<IndexingFilter> filters = ExtensionHelper.getOrderedExtensions(
        conf, IndexingFilter.class, "test.extension.order");
    
    assertNotNull(filters, "Should return ordered list");
    
    // Verify ordering if both extensions are present
    int basicIndex = -1;
    int anchorIndex = -1;
    
    for (int i = 0; i < filters.size(); i++) {
      String className = filters.get(i).getClass().getName();
      if (className.equals("org.apache.nutch.indexer.basic.BasicIndexingFilter")) {
        basicIndex = i;
      } else if (className.equals("org.apache.nutch.indexer.anchor.AnchorIndexingFilter")) {
        anchorIndex = i;
      }
    }
    
    // If both are present, basic should come before anchor based on our order
    if (basicIndex >= 0 && anchorIndex >= 0) {
      assertTrue(basicIndex < anchorIndex, 
          "BasicIndexingFilter should come before AnchorIndexingFilter based on order property");
    }
  }

  /**
   * Test getting extensions with plugin filtering.
   */
  @Test
  public void testGetExtensionsWithFiltering() {
    // Only include protocol plugins
    conf.set("plugin.includes", "protocol-.*");
    
    List<Protocol> protocols = ExtensionHelper.getExtensions(conf, Protocol.class);
    List<Parser> parsers = ExtensionHelper.getExtensions(conf, Parser.class);
    
    // Protocols should be available (matching include pattern)
    assertNotNull(protocols);
    
    // Parsers may be empty or filtered out (not matching include pattern)
    assertNotNull(parsers);
  }

  /**
   * Test getting multiple extension types.
   */
  @Test
  public void testMultipleExtensionTypes() {
    List<Protocol> protocols = ExtensionHelper.getExtensions(conf, Protocol.class);
    List<Parser> parsers = ExtensionHelper.getExtensions(conf, Parser.class);
    List<IndexingFilter> filters = ExtensionHelper.getExtensions(conf, IndexingFilter.class);
    
    // All should be non-null lists
    assertNotNull(protocols, "Protocols should not be null");
    assertNotNull(parsers, "Parsers should not be null");
    assertNotNull(filters, "Filters should not be null");
  }
}
