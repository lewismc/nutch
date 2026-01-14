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
import org.pf4j.Extension;

/**
 * A simple test extension that does NOT implement Configurable.
 * 
 * <p>This is used to test that extensions without Configurable still work correctly
 * with the NutchExtensionFactory.</p>
 */
@Extension
public class SimpleTestExtension implements ITestPF4JExtension {

  /**
   * Default constructor required by PF4J.
   */
  public SimpleTestExtension() {
  }

  @Override
  public String process(String input) {
    return "simple: " + input;
  }

  @Override
  public Configuration getConfiguration() {
    // This extension doesn't support Configuration
    return null;
  }
}
