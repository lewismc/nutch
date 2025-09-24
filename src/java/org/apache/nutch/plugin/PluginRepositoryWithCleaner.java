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

import java.lang.invoke.MethodHandles;
import java.lang.ref.Cleaner;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>A modern implementation of PluginRepository that uses the Cleaner API
 * (Java 9+) to handle resource cleanup without the deprecated finalize() method.</p>
 * 
 * <p>The plugin repository is a registry of all plugins.</p>
 * 
 * <p>At system boot up a repository is built by parsing the manifest files of
 * all plugins. Plugins that require other plugins which do not exist are not
 * registered. For each plugin a plugin descriptor instance will be created. The
 * descriptor represents all meta information about a plugin. So a plugin
 * instance will be created later when it is required, this allow lazy plugin
 * loading.</p>
 *
 * <p>As protocol-plugins need to be registered with the JVM as well, this class
 * also acts as an {@link java.net.URLStreamHandlerFactory} that registers with
 * the JVM and supports all the new protocols as if they were native.</p>
 * 
 * <p>The Cleaner API provides a more reliable and performant alternative to
 * finalization for automatic resource management.</p>
 * 
 * @author Apache Nutch Team
 * @since Nutch 1.20
 */
public class PluginRepositoryWithCleaner implements URLStreamHandlerFactory {
  private static final WeakHashMap<String, PluginRepositoryWithCleaner> CACHE = new WeakHashMap<>();
  private static final Cleaner cleaner = Cleaner.create();

  private boolean auto;
  private List<PluginDescriptor> fRegisteredPlugins;
  private HashMap<String, ExtensionPoint> fExtensionPoints;
  private final Map<String, Plugin> fActivatedPlugins;
  private static final Map<String, Map<PluginClassLoader, Class<?>>> CLASS_CACHE = new HashMap<>();
  private Configuration conf;
  private final Cleaner.Cleanable cleanable;
  private final CleanupState state;

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * State object that holds the resources to be cleaned up.
   * This class must not hold references to the outer PluginRepositoryWithCleaner instance.
   */
  private static class CleanupState implements Runnable {
    private final Map<String, Plugin> activatedPlugins;
    private final String uuid;
    private volatile boolean cleaned = false;

    CleanupState(String uuid) {
      this.activatedPlugins = new ConcurrentHashMap<>();
      this.uuid = uuid;
    }

    void addPlugin(String id, Plugin plugin) {
      activatedPlugins.put(id, plugin);
    }

    void removePlugin(String id) {
      activatedPlugins.remove(id);
    }

    Map<String, Plugin> getPlugins() {
      return new HashMap<>(activatedPlugins);
    }

    @Override
    public void run() {
      if (!cleaned) {
        try {
          // Shut down all activated plugins
          for (Map.Entry<String, Plugin> entry : activatedPlugins.entrySet()) {
            try {
              Plugin plugin = entry.getValue();
              if (plugin != null) {
                plugin.shutDown();
                LOG.debug("Cleaned up plugin: {}", entry.getKey());
              }
            } catch (Exception e) {
              LOG.error("Error shutting down plugin: {}", entry.getKey(), e);
            }
          }
          activatedPlugins.clear();
          
          // Remove from cache
          if (uuid != null) {
            synchronized (CACHE) {
              CACHE.remove(uuid);
            }
          }
        } finally {
          cleaned = true;
        }
      }
    }

    boolean isCleaned() {
      return cleaned;
    }
  }

  /**
   * @param conf a populated {@link Configuration}
   * @throws RuntimeException if a fatal runtime error is encountered 
   */
  public PluginRepositoryWithCleaner(Configuration conf) throws RuntimeException {
    this.conf = new Configuration(conf);
    
    // Calculate UUID for cache management
    String uuid = NutchConfiguration.getUUID(this.conf);
    if (uuid == null) {
      uuid = "nonNutchConf@" + this.conf.hashCode();
    }
    
    // Initialize cleanup state
    this.state = new CleanupState(uuid);
    this.fActivatedPlugins = state.getPlugins();
    this.cleanable = cleaner.register(this, state);
    
    // Initialize repository
    this.fExtensionPoints = new HashMap<>();
    this.auto = conf.getBoolean("plugin.auto-activation", true);
    String[] pluginFolders = conf.getStrings("plugin.folders");
    PluginManifestParser manifestParser = new PluginManifestParser(this.conf, this);
    Map<String, PluginDescriptor> allPlugins = manifestParser.parsePluginFolder(pluginFolders);
    
    if (allPlugins.isEmpty()) {
      LOG.warn("No plugins found on paths of property plugin.folders=\"{}\"",
              conf.get("plugin.folders"));
    }
    
    Pattern excludes = Pattern.compile(conf.get("plugin.excludes", ""));
    Pattern includes = Pattern.compile(conf.get("plugin.includes", ""));
    Map<String, PluginDescriptor> filteredPlugins = filter(excludes, includes, allPlugins);
    this.fRegisteredPlugins = getDependencyCheckedPlugins(filteredPlugins,
            this.auto ? allPlugins : filteredPlugins);
    installExtensionPoints(this.fRegisteredPlugins);
    
    try {
      installExtensions(this.fRegisteredPlugins);
    } catch (PluginRuntimeException e) {
      LOG.error("Could not install extensions.", e.toString());
      throw new RuntimeException(e.getMessage());
    }

    registerURLStreamHandlerFactory();
    displayStatus();
  }

  /**
   * Get a cached instance of the {@link PluginRepositoryWithCleaner}
   * @param conf a populated {@link Configuration}
   * @return a cached instance of the plugin repository
   */
  public static synchronized PluginRepositoryWithCleaner get(Configuration conf) {
    String uuid = NutchConfiguration.getUUID(conf);
    if (uuid == null) {
      uuid = "nonNutchConf@" + conf.hashCode(); // fallback
    }
    PluginRepositoryWithCleaner result = CACHE.get(uuid);
    if (result == null) {
      result = new PluginRepositoryWithCleaner(conf);
      CACHE.put(uuid, result);
    }
    return result;
  }

  /**
   * Explicitly close and clean up the repository resources.
   * This method provides a way to manually trigger cleanup.
   */
  public void close() {
    // Trigger the cleanup immediately
    cleanable.clean();
  }

  /**
   * Shuts down all plugins explicitly.
   * This can be called without triggering the full cleanup.
   * 
   * @throws PluginRuntimeException if an error occurs during shutdown
   */
  public void shutDownActivatedPlugins() throws PluginRuntimeException {
    Map<String, Plugin> plugins = state.getPlugins();
    for (Map.Entry<String, Plugin> entry : plugins.entrySet()) {
      try {
        Plugin plugin = entry.getValue();
        if (plugin != null) {
          plugin.shutDown();
        }
      } catch (Exception e) {
        LOG.error("Error shutting down plugin: {}", entry.getKey(), e);
      }
    }
    plugins.clear();
  }

  /**
   * Check if the repository has been cleaned
   * @return true if the repository has been cleaned, false otherwise
   */
  public boolean isCleaned() {
    return state.isCleaned();
  }

  private void installExtensionPoints(List<PluginDescriptor> plugins) {
    if (plugins == null) {
      return;
    }

    for (PluginDescriptor plugin : plugins) {
      for (ExtensionPoint point : plugin.getExtenstionPoints()) {
        String xpId = point.getId();
        LOG.debug("Adding extension point {}", xpId);
        this.fExtensionPoints.put(xpId, point);
      }
    }
  }

  private void installExtensions(List<PluginDescriptor> pRegisteredPlugins)
          throws PluginRuntimeException {
    for (PluginDescriptor descriptor : pRegisteredPlugins) {
      for (Extension extension : descriptor.getExtensions()) {
        String xpId = extension.getTargetPoint();
        ExtensionPoint point = getExtensionPoint(xpId);
        if (point == null) {
          throw new PluginRuntimeException("Plugin (" + descriptor.getPluginId()
          + "), " + "extension point: " + xpId + " does not exist.");
        }
        point.addExtension(extension);
      }
    }
  }

  private void getPluginCheckedDependencies(PluginDescriptor plugin,
          Map<String, PluginDescriptor> plugins,
          Map<String, PluginDescriptor> dependencies,
          Map<String, PluginDescriptor> branch)
                  throws MissingDependencyException, CircularDependencyException {
    if (dependencies == null) {
      dependencies = new HashMap<>();
    }
    if (branch == null) {
      branch = new HashMap<>();
    }
    branch.put(plugin.getPluginId(), plugin);

    for (String id : plugin.getDependencies()) {
      PluginDescriptor dependency = plugins.get(id);
      if (dependency == null) {
        throw new MissingDependencyException(
                "Missing dependency " + id + " for plugin " + plugin.getPluginId());
      }
      if (branch.containsKey(id)) {
        throw new CircularDependencyException("Circular dependency detected "
                + id + " for plugin " + plugin.getPluginId());
      }
      dependencies.put(id, dependency);
      getPluginCheckedDependencies(plugins.get(id), plugins, dependencies, branch);
    }

    branch.remove(plugin.getPluginId());
  }

  private Map<String, PluginDescriptor> getPluginCheckedDependencies(
          PluginDescriptor plugin, Map<String, PluginDescriptor> plugins)
                  throws MissingDependencyException, CircularDependencyException {
    Map<String, PluginDescriptor> dependencies = new HashMap<>();
    Map<String, PluginDescriptor> branch = new HashMap<>();
    getPluginCheckedDependencies(plugin, plugins, dependencies, branch);
    return dependencies;
  }

  private List<PluginDescriptor> getDependencyCheckedPlugins(
          Map<String, PluginDescriptor> filtered,
          Map<String, PluginDescriptor> all) {
    if (filtered == null) {
      return null;
    }
    Map<String, PluginDescriptor> checked = new HashMap<>();

    for (PluginDescriptor plugin : filtered.values()) {
      try {
        checked.putAll(getPluginCheckedDependencies(plugin, all));
        checked.put(plugin.getPluginId(), plugin);
      } catch (MissingDependencyException mde) {
        LOG.warn(mde.getMessage());
      } catch (CircularDependencyException cde) {
        LOG.warn(cde.getMessage());
      }
    }
    return new ArrayList<>(checked.values());
  }

  public PluginDescriptor[] getPluginDescriptors() {
    return this.fRegisteredPlugins
            .toArray(new PluginDescriptor[this.fRegisteredPlugins.size()]);
  }

  public PluginDescriptor getPluginDescriptor(String pPluginId) {
    for (PluginDescriptor descriptor : this.fRegisteredPlugins) {
      if (descriptor.getPluginId().equals(pPluginId))
        return descriptor;
    }
    return null;
  }

  public ExtensionPoint getExtensionPoint(String pXpId) {
    return this.fExtensionPoints.get(pXpId);
  }

  public Plugin getPluginInstance(PluginDescriptor pDescriptor)
          throws PluginRuntimeException {
    Map<String, Plugin> plugins = state.getPlugins();
    if (plugins.containsKey(pDescriptor.getPluginId())) {
      return plugins.get(pDescriptor.getPluginId());
    }
    
    try {
      synchronized (pDescriptor) {
        // Double-check after acquiring lock
        if (plugins.containsKey(pDescriptor.getPluginId())) {
          return plugins.get(pDescriptor.getPluginId());
        }
        
        Class<?> pluginClass = getCachedClass(pDescriptor,
                pDescriptor.getPluginClass());
        Constructor<?> constructor = pluginClass.getConstructor(
                new Class<?>[] { PluginDescriptor.class, Configuration.class });
        Plugin plugin = (Plugin) constructor
                .newInstance(new Object[] { pDescriptor, this.conf });
        plugin.startUp();
        state.addPlugin(pDescriptor.getPluginId(), plugin);
        return plugin;
      }
    } catch (ClassNotFoundException e) {
      throw new PluginRuntimeException(e);
    } catch (InstantiationException e) {
      throw new PluginRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new PluginRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new PluginRuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new PluginRuntimeException(e);
    }
  }

  public Class<?> getCachedClass(PluginDescriptor pDescriptor, String className)
          throws ClassNotFoundException {
    Map<PluginClassLoader, Class<?>> descMap = CLASS_CACHE.get(className);
    if (descMap == null) {
      descMap = new HashMap<>();
      CLASS_CACHE.put(className, descMap);
    }
    PluginClassLoader loader = pDescriptor.getClassLoader();
    Class<?> clazz = descMap.get(loader);
    if (clazz == null) {
      clazz = loader.loadClass(className);
      descMap.put(loader, clazz);
    }
    return clazz;
  }

  private void displayStatus() {
    LOG.info("Plugin Auto-activation mode: [{}]", this.auto);
    LOG.info("Registered Plugins:");

    if ((this.fRegisteredPlugins == null) || (this.fRegisteredPlugins.size() == 0)) {
      LOG.info("\tNONE");
    } else {
      for (PluginDescriptor plugin : this.fRegisteredPlugins) {
        LOG.info("\t{} ({})", plugin.getName(), plugin.getPluginId());
      }
    }

    LOG.info("Registered Extension-Points:");
    if ((this.fExtensionPoints == null) || (this.fExtensionPoints.size() == 0)) {
      LOG.info("\tNONE");
    } else {
      for (ExtensionPoint ep : this.fExtensionPoints.values()) {
        LOG.info("\t{} ({})", ep.getName(), ep.getId());
      }
    }
  }

  private static Map<String, PluginDescriptor> filter(Pattern excludes,
          Pattern includes, Map<String, PluginDescriptor> plugins) {
    Map<String, PluginDescriptor> map = new HashMap<>();

    if (plugins == null) {
      return map;
    }

    for (PluginDescriptor plugin : plugins.values()) {
      if (plugin == null) {
        continue;
      }
      String id = plugin.getPluginId();
      if (id == null) {
        continue;
      }

      if (!includes.matcher(id).matches()) {
        LOG.debug("not including: {}", id);
        continue;
      }
      if (excludes.matcher(id).matches()) {
        LOG.debug("excluding: {}", id);
        continue;
      }
      map.put(plugin.getPluginId(), plugin);
    }
    return map;
  }

  public synchronized Object[] getOrderedPlugins(Class<?> clazz,
          String xPointId, String orderProperty) {
    Object[] filters;
    ObjectCache objectCache = ObjectCache.get(this.conf);
    filters = (Object[]) objectCache.getObject(clazz.getName());

    if (filters == null) {
      String order = this.conf.get(orderProperty);
      List<String> orderOfFilters = new ArrayList<>();
      boolean userDefinedOrder = false;
      if (order != null && !order.trim().isEmpty()) {
        orderOfFilters = Arrays.asList(order.trim().split("\\s+"));
        userDefinedOrder = true;
      }

      try {
        ExtensionPoint point = PluginRepositoryWithCleaner.get(this.conf)
                .getExtensionPoint(xPointId);
        if (point == null)
          throw new RuntimeException(xPointId + " not found.");
        Extension[] extensions = point.getExtensions();
        HashMap<String, Object> filterMap = new HashMap<>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          Object filter = extension.getExtensionInstance();
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
            if (!userDefinedOrder)
              orderOfFilters.add(filter.getClass().getName());
          }
        }
        List<Object> sorted = new ArrayList<>();
        for (String orderedFilter : orderOfFilters) {
          Object f = filterMap.get(orderedFilter);
          if (f == null) {
            LOG.error("{} : {} declared in configuration property {} "
                    + "but not found in an active plugin - ignoring.", 
                    clazz.getSimpleName(), orderedFilter, orderProperty);
            continue;
          }
          sorted.add(f);
        }
        Object[] filter = (Object[]) Array.newInstance(clazz, sorted.size());
        for (int i = 0; i < sorted.size(); i++) {
          filter[i] = sorted.get(i);
          LOG.trace("{} : filters[{}] = {}", clazz.getSimpleName(), i,
              filter[i].getClass());
        }
        objectCache.setObject(clazz.getName(), filter);
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }

      filters = (Object[]) objectCache.getObject(clazz.getName());
    }
    return filters;
  }

  private void registerURLStreamHandlerFactory() {
    org.apache.nutch.plugin.URLStreamHandlerFactory.getInstance().registerPluginRepository(this);
  }

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    LOG.debug("Creating URLStreamHandler for protocol: {}", protocol);

    if (this.fExtensionPoints != null) {
      ExtensionPoint ep = this.fExtensionPoints
              .get("org.apache.nutch.protocol.Protocol");
      if (ep != null) {
        Extension[] extensions = ep.getExtensions();
        for (Extension extension : extensions) {
          String p = extension.getAttribute("protocolName");
          if (p.equals(protocol)) {
            LOG.debug("Suitable protocolName attribute located: {}", p);

            Object extinst = null;
            try {
              extinst = extension.getExtensionInstance();
              LOG.debug("Located extension instance class: {}", extinst.getClass().getName());
            } catch (Exception e) {
              LOG.warn("Could not find {}", extension.getId(), e);
            }

            String handlerClass = extension.getAttribute("urlStreamHandler");
            if (handlerClass != null) {
              LOG.debug("Located URLStreamHandler: {}", handlerClass);
              ClassLoader cl = this.getClass().getClassLoader();
              if (extinst != null) {
                cl = extinst.getClass().getClassLoader();
              }

              try {
                Class<?> clazz = cl.loadClass(handlerClass);
                return (URLStreamHandler) clazz.getDeclaredConstructor().newInstance();
              } catch (Exception e) {
                LOG.error("Could not instantiate protocol {} handler class {} defined by extension {}", 
                        protocol, handlerClass, extension.getId(), e);
                return null;
              }
            }

            LOG.debug("Suitable protocol extension found that did not declare a handler");
            return null;
          }
        }
        LOG.debug("No suitable protocol extensions registered for protocol: {}", protocol);
      } else {
        LOG.debug("No protocol extensions registered?");
      }
    }

    return null;
  }

  /**
   * Loads all necessary dependencies for a selected plugin, and then runs one
   * of the classes' main() method.
   * 
   * @param args
   *          plugin ID (needs to be activated in the configuration), and the
   *          class name. The rest of arguments is passed to the main method of
   *          the selected class.
   * @throws Exception if there is an error running this Class
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(
              "Usage: PluginRepositoryWithCleaner pluginId className [arg1 arg2 ...]");
      return;
    }
    Configuration conf = NutchConfiguration.create();
    PluginRepositoryWithCleaner repo = new PluginRepositoryWithCleaner(conf);
    
    try {
      // args[0] - plugin ID
      PluginDescriptor d = repo.getPluginDescriptor(args[0]);
      if (d == null) {
        System.err.println("Plugin '" + args[0] + "' not present or inactive.");
        return;
      }
      ClassLoader cl = d.getClassLoader();
      // args[1] - class name
      Class<?> clazz = null;
      try {
        clazz = Class.forName(args[1], true, cl);
      } catch (Exception e) {
        System.err.println(
                "Could not load the class '" + args[1] + ": " + e.getMessage());
        return;
      }
      Method m = null;
      try {
        m = clazz.getMethod("main", new Class<?>[] { args.getClass() });
      } catch (Exception e) {
        System.err.println("Could not find the 'main(String[])' method in class "
                + args[1] + ": " + e.getMessage());
        return;
      }
      String[] subargs = new String[args.length - 2];
      System.arraycopy(args, 2, subargs, 0, subargs.length);
      m.invoke(null, new Object[] { subargs });
    } finally {
      // Explicitly trigger cleanup
      repo.close();
    }
  }
}