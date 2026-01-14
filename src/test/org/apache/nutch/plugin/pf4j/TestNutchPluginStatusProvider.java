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

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NutchPluginStatusProvider}.
 * 
 * <p>Tests the include/exclude pattern filtering logic used to determine
 * which plugins are enabled or disabled based on Nutch configuration.</p>
 */
public class TestNutchPluginStatusProvider {

  /**
   * Test that all plugins are enabled when no patterns are specified.
   */
  @Test
  public void testPluginEnabledWithNoPatterns() {
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(null, null);
    
    // Without any patterns, all plugins should be enabled (not disabled)
    assertFalse(provider.isPluginDisabled("protocol-http"));
    assertFalse(provider.isPluginDisabled("parse-html"));
    assertFalse(provider.isPluginDisabled("index-basic"));
    assertFalse(provider.isPluginDisabled("any-plugin-name"));
  }

  /**
   * Test that plugins matching the include pattern are enabled.
   */
  @Test
  public void testPluginMatchesIncludePattern() {
    Pattern includePattern = Pattern.compile("protocol-.*");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, null);
    
    // Plugins matching the include pattern should be enabled
    assertFalse(provider.isPluginDisabled("protocol-http"));
    assertFalse(provider.isPluginDisabled("protocol-ftp"));
    assertFalse(provider.isPluginDisabled("protocol-httpclient"));
  }

  /**
   * Test that plugins matching the exclude pattern are disabled.
   */
  @Test
  public void testPluginExcludedByPattern() {
    Pattern excludePattern = Pattern.compile(".*-test");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(null, excludePattern);
    
    // Plugins matching the exclude pattern should be disabled
    assertTrue(provider.isPluginDisabled("plugin-test"));
    assertTrue(provider.isPluginDisabled("my-test"));
    
    // Plugins not matching exclude should be enabled
    assertFalse(provider.isPluginDisabled("protocol-http"));
    assertFalse(provider.isPluginDisabled("test-plugin")); // doesn't end with -test
  }

  /**
   * Test that exclude pattern takes precedence over include pattern.
   */
  @Test
  public void testExcludePatternTakesPrecedence() {
    Pattern includePattern = Pattern.compile("protocol-.*");
    Pattern excludePattern = Pattern.compile("protocol-ftp");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, excludePattern);
    
    // protocol-http matches include, doesn't match exclude - should be enabled
    assertFalse(provider.isPluginDisabled("protocol-http"));
    
    // protocol-ftp matches both include and exclude - exclude takes precedence
    assertTrue(provider.isPluginDisabled("protocol-ftp"));
  }

  /**
   * Test that plugins not matching include pattern are disabled when include pattern exists.
   */
  @Test
  public void testPluginNotMatchingIncludeIsDisabled() {
    Pattern includePattern = Pattern.compile("protocol-.*");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, null);
    
    // Plugins not matching include pattern should be disabled
    assertTrue(provider.isPluginDisabled("parse-html"));
    assertTrue(provider.isPluginDisabled("index-basic"));
    assertTrue(provider.isPluginDisabled("urlfilter-regex"));
  }

  /**
   * Test with complex regex patterns similar to Nutch configuration.
   */
  @Test
  public void testComplexPatterns() {
    // Pattern similar to default Nutch plugin.includes
    Pattern includePattern = Pattern.compile("protocol-(http|httpclient)|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|scoring-opic|urlnormalizer-(pass|regex|basic)");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, null);
    
    // Should be enabled
    assertFalse(provider.isPluginDisabled("protocol-http"));
    assertFalse(provider.isPluginDisabled("protocol-httpclient"));
    assertFalse(provider.isPluginDisabled("urlfilter-regex"));
    assertFalse(provider.isPluginDisabled("parse-html"));
    assertFalse(provider.isPluginDisabled("parse-tika"));
    assertFalse(provider.isPluginDisabled("index-basic"));
    assertFalse(provider.isPluginDisabled("index-anchor"));
    assertFalse(provider.isPluginDisabled("scoring-opic"));
    assertFalse(provider.isPluginDisabled("urlnormalizer-pass"));
    assertFalse(provider.isPluginDisabled("urlnormalizer-regex"));
    assertFalse(provider.isPluginDisabled("urlnormalizer-basic"));
    
    // Should be disabled (not in pattern)
    assertTrue(provider.isPluginDisabled("protocol-ftp"));
    assertTrue(provider.isPluginDisabled("parse-zip"));
    assertTrue(provider.isPluginDisabled("index-more"));
    assertTrue(provider.isPluginDisabled("scoring-depth"));
  }

  /**
   * Test that wildcard patterns work correctly.
   */
  @Test
  public void testWildcardPattern() {
    Pattern includePattern = Pattern.compile(".*");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, null);
    
    // All plugins should match the wildcard
    assertFalse(provider.isPluginDisabled("any-plugin"));
    assertFalse(provider.isPluginDisabled("protocol-http"));
    assertFalse(provider.isPluginDisabled(""));
  }

  /**
   * Test that dynamic enable/disable operations log warnings but don't throw.
   */
  @Test
  public void testDynamicEnableDisableLogsWarning() {
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(null, null);
    
    // These should not throw exceptions, just log warnings
    assertDoesNotThrow(() -> provider.disablePlugin("some-plugin"));
    assertDoesNotThrow(() -> provider.enablePlugin("some-plugin"));
    
    // Status should remain unchanged (based on patterns only)
    assertFalse(provider.isPluginDisabled("some-plugin"));
  }

  /**
   * Test with empty string plugin ID.
   */
  @Test
  public void testEmptyPluginId() {
    Pattern includePattern = Pattern.compile("protocol-.*");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, null);
    
    // Empty string doesn't match the pattern, so should be disabled
    assertTrue(provider.isPluginDisabled(""));
  }

  /**
   * Test with special characters in plugin ID.
   */
  @Test
  public void testSpecialCharactersInPluginId() {
    Pattern includePattern = Pattern.compile("my\\.plugin");
    NutchPluginStatusProvider provider = new NutchPluginStatusProvider(includePattern, null);
    
    // Pattern with escaped dot should match literal dot
    assertFalse(provider.isPluginDisabled("my.plugin"));
    assertTrue(provider.isPluginDisabled("myXplugin")); // X doesn't match escaped dot
  }
}
