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

import org.pf4j.ExtensionPoint;

/**
 * Marker interface for Nutch extension points using PF4J.
 * 
 * <p>This interface extends PF4J's {@link ExtensionPoint} and serves as the
 * base for all Nutch extension point interfaces. It replaces the legacy
 * {@link org.apache.nutch.plugin.Pluggable} interface.</p>
 * 
 * <p>All Nutch extension point interfaces (Protocol, Parser, IndexingFilter, etc.)
 * should extend this interface to be discoverable by PF4J.</p>
 * 
 * <p>Example:
 * <pre>
 * public interface Protocol extends NutchExtensionPoint, Configurable {
 *     // Protocol methods
 * }
 * </pre>
 * </p>
 * 
 * @see org.pf4j.ExtensionPoint
 * @see org.apache.nutch.plugin.Pluggable
 */
public interface NutchExtensionPoint extends ExtensionPoint {
  // Marker interface - no methods required
}
