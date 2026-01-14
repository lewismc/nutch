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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Pluggable;

/**
 * Test extension point interface for PF4J integration tests.
 * 
 * <p>This interface extends {@link Pluggable} (which extends {@link org.pf4j.ExtensionPoint})
 * making it a valid PF4J extension point that can be discovered by the plugin manager.</p>
 */
public interface ITestPF4JExtension extends Pluggable {

  /**
   * Process input and return a result.
   * 
   * @param input the input string to process
   * @return the processed result
   */
  String process(String input);

  /**
   * Get the Hadoop Configuration that was injected into this extension.
   * 
   * @return the Configuration object, or null if not configured
   */
  Configuration getConfiguration();
}
