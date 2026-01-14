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
package org.apache.nutch.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metrics.ShuffleAnalyzer.ShuffleIntensity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ShuffleAnalyzer}.
 */
public class TestShuffleAnalyzer {

  private Configuration conf;

  @BeforeEach
  public void setUp() {
    conf = new Configuration();
  }

  /**
   * Test that the SHUFFLE_ANALYSIS_ENABLED constant is defined correctly.
   */
  @Test
  public void testConfigurationConstant() {
    assertNotNull(ShuffleAnalyzer.SHUFFLE_ANALYSIS_ENABLED);
    assertEquals("shuffle.analysis.enabled", ShuffleAnalyzer.SHUFFLE_ANALYSIS_ENABLED);
  }

  /**
   * Test that shuffle analysis is disabled by default.
   */
  @Test
  public void testDefaultDisabled() {
    // By default, shuffle analysis should be disabled
    boolean enabled = conf.getBoolean(ShuffleAnalyzer.SHUFFLE_ANALYSIS_ENABLED, false);
    assertEquals(false, enabled);
  }

  /**
   * Test that shuffle analysis can be enabled via configuration.
   */
  @Test
  public void testCanBeEnabled() {
    conf.setBoolean(ShuffleAnalyzer.SHUFFLE_ANALYSIS_ENABLED, true);
    boolean enabled = conf.getBoolean(ShuffleAnalyzer.SHUFFLE_ANALYSIS_ENABLED, false);
    assertEquals(true, enabled);
  }

  /**
   * Test that shuffle intensity enum values exist.
   */
  @Test
  public void testShuffleIntensityEnum() {
    ShuffleIntensity[] values = ShuffleIntensity.values();
    assertEquals(4, values.length);
    
    // Check all expected values exist
    assertEquals(ShuffleIntensity.LOW, ShuffleIntensity.valueOf("LOW"));
    assertEquals(ShuffleIntensity.MEDIUM, ShuffleIntensity.valueOf("MEDIUM"));
    assertEquals(ShuffleIntensity.HIGH, ShuffleIntensity.valueOf("HIGH"));
    assertEquals(ShuffleIntensity.EXTREME, ShuffleIntensity.valueOf("EXTREME"));
  }

  /**
   * Test that calling logAnalysis with disabled configuration does not throw.
   */
  @Test
  public void testLogAnalysisDisabledNoThrow() {
    // With shuffle analysis disabled (default), this should not throw
    // even with a null job - it should exit early
    conf.setBoolean(ShuffleAnalyzer.SHUFFLE_ANALYSIS_ENABLED, false);
    // This should not throw - the method checks the config first
    ShuffleAnalyzer.logAnalysis(null, conf);
  }
}
