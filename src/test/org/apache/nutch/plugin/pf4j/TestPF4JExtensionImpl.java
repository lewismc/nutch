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
import org.pf4j.Extension;

/**
 * Test implementation of {@link ITestPF4JExtension} annotated with {@code @Extension}.
 * 
 * <p>This class is used to verify that:
 * <ul>
 *   <li>PF4J discovers annotated extensions correctly</li>
 *   <li>Configuration injection works via {@link NutchExtensionFactory}</li>
 *   <li>The extension can be retrieved via {@link ExtensionHelper}</li>
 * </ul>
 * </p>
 */
@Extension
public class TestPF4JExtensionImpl implements ITestPF4JExtension, Configurable {

  private Configuration conf;

  /**
   * Default constructor required by PF4J.
   */
  public TestPF4JExtensionImpl() {
  }

  @Override
  public String process(String input) {
    if (input == null) {
      return "null";
    }
    return input + " processed";
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
