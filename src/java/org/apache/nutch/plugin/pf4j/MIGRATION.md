# Nutch PF4J Plugin Development Guide

This document describes how to develop Nutch plugins using PF4J (Plugin Framework for Java).

## Overview

Apache Nutch uses PF4J 3.14.1, a modern, lightweight, annotation-based plugin framework.
This provides:

- **Better IDE support**: Annotations are easier to navigate and refactor
- **Type safety**: Compile-time checking of extension points
- **Standard framework**: Leverage PF4J's active community and documentation
- **Simplified plugin development**: Less boilerplate configuration

## Developing Plugins

### Step 1: Add the @Extension Annotation

Mark your extension classes with the `@Extension` annotation:

```java
import org.pf4j.Extension;
import org.apache.nutch.protocol.Protocol;

@Extension
public class Http extends HttpBase implements Protocol {
    // Implementation
}
```

### Step 2: Create plugin.properties

Create a `plugin.properties` file in your plugin's root directory:

```properties
# Plugin metadata
plugin.id=protocol-http
plugin.version=1.0.0
plugin.description=HTTP Protocol Plugin
plugin.provider=Apache Nutch
plugin.license=Apache License 2.0

# Optional: specify dependencies
plugin.dependencies=lib-http

# Optional: specify the plugin class (defaults to NutchPlugin)
plugin.class=org.apache.nutch.plugin.pf4j.NutchPlugin
```

### Step 3: Ensure Interface Implementation

Make sure your extension class explicitly implements the extension point interface:

```java
// Good - explicit interface
@Extension
public class Http extends HttpBase implements Protocol {
}

// The interface implementation is required for PF4J to discover the extension
```

### Step 4: Build Configuration

The Nutch build system handles plugin compilation automatically for plugins under `src/plugin/`.

For custom build configurations, ensure they:

1. Include the `plugin.properties` file in the JAR
2. Run the PF4J annotation processor during compilation
3. Include the PF4J dependency in your classpath

## Plugin System Components

| Component | Description |
|-----------|-------------|
| `@Extension` annotation | Marks a class as a plugin extension |
| `plugin.properties` | Plugin metadata file |
| Extension point interface | Java interface extending `Pluggable` |
| `NutchPluginManager` | Manages plugin discovery and lifecycle |
| `NutchExtensionFactory` | Creates extensions with Configuration injection |

## Configuration (Hadoop)

Nutch plugins are configured via Hadoop's Configuration object:

```java
@Extension
public class MyPlugin implements SomeExtensionPoint {
    private Configuration conf;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
```

## Plugin Filtering

Plugin filtering via `plugin.includes` and `plugin.excludes` properties controls
which plugins are enabled. The `NutchPluginStatusProvider` integrates with Hadoop
Configuration to respect these settings.

## Legacy Classes

The following classes are deprecated and remain for backward compatibility:

- `PluginManifestParser` - Use `plugin.properties` instead of `plugin.xml`
- `ExtensionPoint` - Use interfaces extending `org.pf4j.ExtensionPoint`
- `Extension` - Use `@org.pf4j.Extension` annotation

## Example: Complete Plugin

**plugin.properties:**
```properties
plugin.id=myplugin
plugin.version=1.0.0
plugin.provider=Example
plugin.dependencies=nutch-extensionpoints
```

**Java class:**
```java
import org.pf4j.Extension;
import org.apache.nutch.protocol.Protocol;

@Extension
public class MyProtocol implements Protocol {
    // Implementation
}
```

## Troubleshooting

### Extension not discovered

1. Verify `@Extension` annotation is present
2. Ensure the class implements the extension point interface
3. Check that `plugin.properties` exists and has correct `plugin.id`
4. Verify the plugin is not excluded via `plugin.excludes`

### ClassNotFoundException

1. Verify all dependencies are declared in `plugin.properties`
2. Check that dependent JARs are present in the plugin's lib directory

### Configuration not available

Ensure your extension class implements `Configurable` and the `setConf`/`getConf` methods:

```java
@Extension
public class MyPlugin implements SomeExtensionPoint, Configurable {
    private Configuration conf;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
```

## Resources

- [PF4J Documentation](https://pf4j.org/)
- [PF4J GitHub Repository](https://github.com/pf4j/pf4j)
- [Nutch Plugin Development Guide](https://nutch.apache.org/documentation/)
