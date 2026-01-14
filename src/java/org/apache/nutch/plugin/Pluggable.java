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
package org.apache.nutch.plugin;

import org.pf4j.ExtensionPoint;

/**
 * Defines the capability of a class to be plugged into Nutch. This is a common
 * interface that must be implemented by all Nutch Extension Points.
 * 
 * <p>This interface now extends PF4J's {@link ExtensionPoint} to support both
 * the legacy XML-based plugin system and the new PF4J annotation-based system.
 * Existing plugins continue to work unchanged.</p>
 * 
 * @author J&eacute;r&ocirc;me Charron
 * 
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/AboutPlugins">About Plugins</a>
 * @see <a href="package-summary.html#package_description"> plugin package
 *      description</a>
 * @see org.pf4j.ExtensionPoint
 */
public interface Pluggable extends ExtensionPoint {

}
