/**
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

package org.apache.nutch.crawl;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapred.JobConf;

/**
 * Utility to test transitions of {@link CrawlDatum} states during an update of
 * {@link CrawlDb} (command {@literal updatedb}): call
 * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)} with
 * the old CrawlDatum (db status) and the new one (fetch status)
 */
public class CrawlDbUpdateUtil {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private CrawlDbReducer reducer;

  public static Text dummyURL = new Text("http://nutch.apache.org/");

  protected CrawlDbUpdateUtil(CrawlDbReducer red, Configuration conf) {
    reducer = red;
    reducer.configure(Job.getInstance(conf));
  }

  /** {@link Context} to collect all values in a {@link List} */
  private class DummyContext extends Context {

    private List<CrawlDatum> values = new ArrayList<CrawlDatum>();

    public void write(Text key, CrawlDatum value) throws IOException, InterruptedException {
      values.add(value);
    }

    /** collected values as list */
    public List<CrawlDatum> getValues() {
      return values;
    }

    private Counters dummyCounters = new Counters();

    public void progress() {
    }

    public Counter getCounter(Enum<?> arg0) {
      return dummyCounters.getGroup("dummy").getCounterForName("dummy");
    }

    public Counter getCounter(String arg0, String arg1) {
      return dummyCounters.getGroup("dummy").getCounterForName("dummy");
    }

    public InputSplit getInputSplit() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Dummy context without input");
    }

    public void setStatus(String arg0) {
    }

    public float getProgress() {
      return 1f;
    }
    
    public OutputCommitter getOutputCommitter() {
      throw new UnsupportedOperationException("Dummy context without committer");
    }

    public boolean nextKey(){
      return false;
    }
  }

  /**
   * run
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
   * and return the CrawlDatum(s) which would have been written into CrawlDb
   * 
   * @param values
   *          list of input CrawlDatums
   * @return list of resulting CrawlDatum(s) in CrawlDb
   */
  public List<CrawlDatum> update(List<CrawlDatum> values) {
    if (values == null || values.size() == 0) {
      return new ArrayList<CrawlDatum>(0);
    }
    Collections.shuffle(values); // sorting of values should have no influence
    DummyContext context = new DummyContext();
    try {
      reducer.reduce(dummyURL, (Iterator)values, context);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return context.getValues();
  }

  /**
   * run
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
   * and return the CrawlDatum(s) which would have been written into CrawlDb
   * 
   * @param dbDatum
   *          previous CrawlDatum in CrawlDb
   * @param fetchDatum
   *          CrawlDatum resulting from fetching
   * @return list of resulting CrawlDatum(s) in CrawlDb
   */
  public List<CrawlDatum> update(CrawlDatum dbDatum, CrawlDatum fetchDatum) {
    List<CrawlDatum> values = new ArrayList<CrawlDatum>();
    if (dbDatum != null)
      values.add(dbDatum);
    if (fetchDatum != null)
      values.add(fetchDatum);
    return update(values);
  }

  /**
   * see {@link #update(List)}
   */
  public List<CrawlDatum> update(CrawlDatum... values) {
    return update(Arrays.asList(values));
  }

}
