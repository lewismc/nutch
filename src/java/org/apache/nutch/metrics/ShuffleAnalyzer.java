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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for analyzing shuffle metrics from completed MapReduce jobs.
 * 
 * <p>This class extracts Hadoop's built-in TaskCounter and FileSystemCounter
 * values to calculate shuffle intensity metrics that help identify jobs that
 * would benefit from remote shuffle services like Apache Uniffle or Celeborn.
 * 
 * <p>Usage:
 * <pre>
 * // After job.waitForCompletion(true) succeeds:
 * ShuffleAnalyzer.logAnalysis(job, conf);
 * </pre>
 * 
 * <p>The analysis is only performed if the configuration property
 * {@code shuffle.analysis.enabled} is set to {@code true}.
 * 
 * @since 1.22
 */
public final class ShuffleAnalyzer {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /** Configuration key to enable shuffle analysis logging. */
  public static final String SHUFFLE_ANALYSIS_ENABLED = "shuffle.analysis.enabled";

  /** Hadoop FileSystemCounters group name. */
  private static final String FS_COUNTERS = "org.apache.hadoop.mapreduce.FileSystemCounter";

  /** Separator line for log output. */
  private static final String SEPARATOR = "============================================================";

  /** Shuffle intensity thresholds. */
  private static final double SHUFFLE_RATIO_HIGH = 2.0;
  private static final double SHUFFLE_RATIO_EXTREME = 5.0;
  private static final double SPILL_RATIO_HIGH = 2.0;
  private static final double SPILL_RATIO_EXTREME = 3.0;
  private static final double LOCAL_IO_RATIO_HIGH = 3.0;

  private ShuffleAnalyzer() {
    // Utility class - prevent instantiation
  }

  /**
   * Shuffle intensity categories.
   */
  public enum ShuffleIntensity {
    LOW, MEDIUM, HIGH, EXTREME
  }

  /**
   * Logs shuffle analysis for a completed job if enabled in configuration.
   * 
   * @param job the completed Hadoop job
   * @param conf the Nutch configuration
   */
  public static void logAnalysis(Job job, Configuration conf) {
    if (!conf.getBoolean(SHUFFLE_ANALYSIS_ENABLED, false)) {
      return;
    }

    try {
      String jobName = job.getJobName();
      Counters counters = job.getCounters();
      
      if (counters == null) {
        LOG.warn("ShuffleAnalyzer: No counters available for job {}", jobName);
        return;
      }

      // Extract counters
      long mapOutputBytes = getCounterValue(counters, TaskCounter.MAP_OUTPUT_BYTES);
      long mapOutputRecords = getCounterValue(counters, TaskCounter.MAP_OUTPUT_RECORDS);
      long reduceShuffleBytes = getCounterValue(counters, TaskCounter.REDUCE_SHUFFLE_BYTES);
      long spilledRecords = getCounterValue(counters, TaskCounter.SPILLED_RECORDS);
      
      long hdfsRead = getFileSystemCounter(counters, "HDFS_BYTES_READ");
      long hdfsWritten = getFileSystemCounter(counters, "HDFS_BYTES_WRITTEN");
      long fileRead = getFileSystemCounter(counters, "FILE_BYTES_READ");
      long fileWritten = getFileSystemCounter(counters, "FILE_BYTES_WRITTEN");

      // Calculate metrics
      double shuffleRatio = calculateRatio(mapOutputBytes, hdfsRead);
      double spillRatio = calculateRatio(spilledRecords, mapOutputRecords);
      double shuffleAmplification = calculateRatio(mapOutputBytes, hdfsWritten);
      double localIoRatio = calculateRatio(fileRead + fileWritten, hdfsRead + hdfsWritten);

      // Determine intensity
      ShuffleIntensity intensity = determineIntensity(shuffleRatio, spillRatio, localIoRatio);

      // Generate recommendations
      List<String> recommendations = generateRecommendations(intensity, shuffleRatio, spillRatio, 
          localIoRatio, mapOutputBytes, reduceShuffleBytes, mapOutputRecords);

      // Log the analysis
      logReport(jobName, mapOutputBytes, mapOutputRecords, reduceShuffleBytes,
          spilledRecords, hdfsRead, hdfsWritten, fileRead, fileWritten,
          shuffleRatio, spillRatio, shuffleAmplification, localIoRatio,
          intensity, recommendations);

    } catch (IOException e) {
      LOG.warn("ShuffleAnalyzer: Failed to analyze job counters: {}", e.getMessage());
    }
  }

  /**
   * Gets a TaskCounter value from the counters.
   */
  private static long getCounterValue(Counters counters, TaskCounter counter) {
    Counter c = counters.findCounter(counter);
    return c != null ? c.getValue() : 0;
  }

  /**
   * Gets a FileSystemCounter value from the counters.
   */
  private static long getFileSystemCounter(Counters counters, String name) {
    CounterGroup group = counters.getGroup(FS_COUNTERS);
    if (group != null) {
      Counter c = group.findCounter(name);
      if (c != null) {
        return c.getValue();
      }
    }
    return 0;
  }

  /**
   * Calculates a ratio, handling division by zero.
   */
  private static double calculateRatio(double numerator, double denominator) {
    if (denominator == 0) {
      return 0.0;
    }
    return numerator / denominator;
  }

  /**
   * Determines the shuffle intensity category based on metrics.
   */
  private static ShuffleIntensity determineIntensity(double shuffleRatio, 
      double spillRatio, double localIoRatio) {
    
    if (shuffleRatio > SHUFFLE_RATIO_EXTREME || spillRatio > SPILL_RATIO_EXTREME) {
      return ShuffleIntensity.EXTREME;
    } else if (shuffleRatio > SHUFFLE_RATIO_HIGH || spillRatio > SPILL_RATIO_HIGH) {
      return ShuffleIntensity.HIGH;
    } else if (shuffleRatio > 1.0 || spillRatio > 1.0) {
      return ShuffleIntensity.MEDIUM;
    } else {
      return ShuffleIntensity.LOW;
    }
  }

  /**
   * Generates recommendations based on the analysis.
   */
  private static List<String> generateRecommendations(ShuffleIntensity intensity,
      double shuffleRatio, double spillRatio, double localIoRatio,
      long mapOutputBytes, long reduceShuffleBytes, long mapOutputRecords) {
    
    List<String> recommendations = new ArrayList<>();

    // Remote shuffle service recommendation
    if (intensity == ShuffleIntensity.HIGH || intensity == ShuffleIntensity.EXTREME) {
      recommendations.add("This job would BENEFIT from remote shuffle service");
      recommendations.add("Consider Apache Uniffle or Celeborn");
    } else {
      recommendations.add("Remote shuffle service would provide minimal benefit");
    }

    // Spill ratio tuning
    if (spillRatio > SPILL_RATIO_HIGH) {
      recommendations.add("High spill ratio - increase mapreduce.task.io.sort.mb");
      if (spillRatio > SPILL_RATIO_EXTREME) {
        recommendations.add("Consider increasing mapreduce.task.io.sort.factor for faster merges");
      }
    }

    // Local I/O bound detection
    if (localIoRatio > LOCAL_IO_RATIO_HIGH) {
      recommendations.add("Heavy local disk I/O - shuffle is disk-bound");
      recommendations.add("Consider faster local storage (SSD) or more reducers to distribute load");
    }

    // Combiner suggestion - high shuffle ratio suggests map output could be reduced
    if (shuffleRatio > SHUFFLE_RATIO_HIGH && mapOutputRecords > 0) {
      recommendations.add("High shuffle ratio - consider adding a Combiner to reduce map output");
    }

    // Compression suggestion for large shuffle data
    if (mapOutputBytes > 1_000_000_000L) { // > 1GB map output
      recommendations.add("Large map output - ensure mapreduce.map.output.compress=true");
      recommendations.add("Consider LZ4 or Snappy codec for faster compression");
    }

    // Map-only job detection (no shuffle)
    if (reduceShuffleBytes == 0 && mapOutputBytes > 0) {
      recommendations.add("Map-only job detected - no shuffle optimization needed");
    }

    // Network vs disk bottleneck hint
    if (shuffleRatio > SHUFFLE_RATIO_HIGH && localIoRatio < 1.0) {
      recommendations.add("Shuffle appears network-bound rather than disk-bound");
      recommendations.add("Remote shuffle service would offload network pressure from compute nodes");
    }

    // Memory tuning for moderate intensity
    if (intensity == ShuffleIntensity.MEDIUM) {
      recommendations.add("Moderate shuffle - tune mapreduce.reduce.shuffle.input.buffer.percent");
    }

    return recommendations;
  }

  /**
   * Logs the formatted shuffle analysis report.
   */
  private static void logReport(String jobName, long mapOutputBytes, long mapOutputRecords,
      long reduceShuffleBytes, long spilledRecords, long hdfsRead, long hdfsWritten,
      long fileRead, long fileWritten, double shuffleRatio, double spillRatio,
      double shuffleAmplification, double localIoRatio, ShuffleIntensity intensity,
      List<String> recommendations) {

    NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
    
    StringBuilder sb = new StringBuilder();
    sb.append("\n").append(SEPARATOR);
    sb.append("\nSHUFFLE ANALYSIS: ").append(jobName);
    sb.append("\n").append(SEPARATOR);
    
    sb.append("\n\nRAW COUNTERS:");
    sb.append(String.format(Locale.ROOT, "\n  Map Output:         %s bytes (%s records)", 
        nf.format(mapOutputBytes), nf.format(mapOutputRecords)));
    sb.append(String.format(Locale.ROOT, "\n  Reduce Shuffle:     %s bytes", 
        nf.format(reduceShuffleBytes)));
    sb.append(String.format(Locale.ROOT, "\n  Spilled Records:    %s", 
        nf.format(spilledRecords)));
    sb.append(String.format(Locale.ROOT, "\n  HDFS Read:          %s bytes", 
        nf.format(hdfsRead)));
    sb.append(String.format(Locale.ROOT, "\n  HDFS Written:       %s bytes", 
        nf.format(hdfsWritten)));
    sb.append(String.format(Locale.ROOT, "\n  Local File Read:    %s bytes", 
        nf.format(fileRead)));
    sb.append(String.format(Locale.ROOT, "\n  Local File Written: %s bytes", 
        nf.format(fileWritten)));

    sb.append("\n\nCALCULATED METRICS:");
    sb.append(String.format(Locale.ROOT, "\n  Shuffle Ratio:        %.2fx (map output / hdfs input)", 
        shuffleRatio));
    sb.append(String.format(Locale.ROOT, "\n  Spill Ratio:          %.2fx (spills / map records)", 
        spillRatio));
    sb.append(String.format(Locale.ROOT, "\n  Shuffle Amplification: %.2fx", 
        shuffleAmplification));
    sb.append(String.format(Locale.ROOT, "\n  Local I/O Ratio:      %.2fx", 
        localIoRatio));

    sb.append("\n\nSHUFFLE INTENSITY: ").append(intensity.name());

    sb.append("\n\nRECOMMENDATIONS:");
    for (String rec : recommendations) {
      sb.append("\n  * ").append(rec);
    }
    
    sb.append("\n").append(SEPARATOR);

    LOG.info(sb.toString());
  }
}
