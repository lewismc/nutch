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
package org.apache.nutch.service;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.cxf.binding.BindingFactoryManager;
import org.apache.cxf.jaxrs.JAXRSBindingFactory;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.lifecycle.ResourceProvider;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.nutch.fetcher.FetchNodeDb;
import org.apache.nutch.service.impl.ConfManagerImpl;
import org.apache.nutch.service.impl.JobFactory;
import org.apache.nutch.service.impl.JobManagerImpl;
import org.apache.nutch.service.impl.NutchServerPoolExecutor;
import org.apache.nutch.service.impl.SeedManagerImpl;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;
import org.apache.nutch.service.resources.AdminResource;
import org.apache.nutch.service.resources.ConfigResource;
import org.apache.nutch.service.resources.DbResource;
import org.apache.nutch.service.resources.JobResource;
import org.apache.nutch.service.resources.ReaderResouce;
import org.apache.nutch.service.resources.SeedResource;
import org.apache.nutch.service.resources.ServicesResource;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.env.BasicIniEnvironment;
import org.apache.shiro.env.Environment;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.web.jaxrs.ShiroFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.Queues;

public class NutchServer {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final String LOCALHOST = "localhost";
  private static final Integer DEFAULT_PORT = 8081;
  private static final int JOB_CAPACITY = 100;

  private static Integer port = DEFAULT_PORT;
  private static String host  = LOCALHOST;

  private static final String CMD_HELP = "help";
  private static final String CMD_PORT = "port";
  private static final String CMD_HOST = "host";

  private long started;
  private boolean running;
  private ConfManager configManager;
  private JobManager jobManager;
  private SeedManager seedManager;
  private JAXRSServerFactoryBean sf; 
  private SecurityManager securityManager;

  private static FetchNodeDb fetchNodeDb;

  private static NutchServer server;

  static {
    server = new NutchServer();
  }

  private NutchServer() {
    this.configManager = new ConfManagerImpl();
    this.seedManager = new SeedManagerImpl();
    BlockingQueue<Runnable> runnables = Queues.newArrayBlockingQueue(JOB_CAPACITY);
    NutchServerPoolExecutor executor = new NutchServerPoolExecutor(10, JOB_CAPACITY, 1, TimeUnit.HOURS, runnables);
    this.jobManager = new JobManagerImpl(new JobFactory(), this.configManager, executor);
    fetchNodeDb = FetchNodeDb.getInstance();

    //Apache Shiro security see https://issues.apache.org/jira/browse/NUTCH-2925 
    Environment env = new BasicIniEnvironment("classpath:shiro.ini");
    SecurityManager securityManager = env.getSecurityManager(); 
    SecurityUtils.setSecurityManager(securityManager);
    
    this.sf = new JAXRSServerFactoryBean();
    BindingFactoryManager manager = this.sf.getBus().getExtension(BindingFactoryManager.class);
    JAXRSBindingFactory factory = new JAXRSBindingFactory();
    factory.setBus(this.sf.getBus());
    manager.registerBindingFactory(JAXRSBindingFactory.JAXRS_BINDING_ID, factory);
    this.sf.setResourceClasses(getClasses());
    this.sf.setResourceProviders(getResourceProviders());
    this.sf.setProviders(getProviders());
  }

  public static NutchServer getInstance() {
    return server;
  }

  protected static void startServer() {
    server.start();
  }

  private void start() {
    LOG.info("Starting NutchServer on {}:{} ...", host, port);
    try{
      String address = "http://" + host + ":" + port;
      this.sf.setAddress(address);
      this.sf.create();
    }catch(Exception e){
      throw new IllegalStateException("Server could not be started", e);
    }

    this.started = System.currentTimeMillis();
    this.running = true;
    LOG.info("Started Nutch Server on {}:{} at {}", new Object[] {host, port, this.started});
  }

  private static List<Class<?>> getClasses() {
    List<Class<?>> resources = new ArrayList<>();
    resources.add(AdminResource.class);
    resources.add(ConfigResource.class);
    resources.add(DbResource.class);
    resources.add(JobResource.class);
    resources.add(ReaderResouce.class);
    resources.add(SeedResource.class);
    resources.add(ServicesResource.class);
    return resources;
  }

  private static List<Object> getProviders() {
    List<Object> providers = new ArrayList<>();
    providers.add(new JacksonJaxbJsonProvider());
    providers.add(new ShiroFeature());
    return providers;
  }

  private List<ResourceProvider> getResourceProviders() {
    List<ResourceProvider> resourceProviders = new ArrayList<>();
    resourceProviders.add(new SingletonResourceProvider(getConfManager()));
    return resourceProviders;
  }

  public ConfManager getConfManager() {
    return this.configManager;
  }

  public JobManager getJobManager() {
    return this.jobManager;
  }
  
  public SeedManager getSeedManager() {
    return this.seedManager;
  }

  public FetchNodeDb getFetchNodeDb(){
    return fetchNodeDb;
  }

  public boolean isRunning(){
    return this.running;
  }

  public long getStarted(){
    return this.started;
  }

  public static void main(String[] args) throws ParseException {
    CommandLineParser parser = new PosixParser();
    Options options = createOptions();
    CommandLine commandLine = parser.parse(options, args);
    if (commandLine.hasOption(CMD_HELP)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("NutchServer", options, true);
      return;
    }

    if (commandLine.hasOption(CMD_PORT)) {
      port = Integer.parseInt(commandLine.getOptionValue(CMD_PORT));
    }

    if (commandLine.hasOption(CMD_HOST)) {
      host = commandLine.getOptionValue(CMD_HOST);
    }

    startServer();
  }

  private static Options createOptions() {
    Options options = new Options();

    OptionBuilder.withDescription("Show this help");
    options.addOption(OptionBuilder.create(CMD_HELP));

    OptionBuilder.withArgName("port");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withDescription("The port to run the Nutch Server. Default port 8081");
    options.addOption(OptionBuilder.create(CMD_PORT));

    OptionBuilder.withArgName("host");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withDescription("The host to bind the Nutch Server to. Default is localhost.");
    options.addOption(OptionBuilder.create(CMD_HOST));

    return options;
  }

  public boolean canStop(boolean force){
    if(force)
      return true;

    Collection<JobInfo> jobs = getJobManager().list(null, State.RUNNING);
    return jobs.isEmpty();
  }

  protected static void setPort(int port) {
	  NutchServer.port = port;
  }
  
  public int getPort() {
    return port;
  }

  public void stop() {
    System.exit(0);
  }

  public SecurityManager getSecurityManager() {
    return this.securityManager;
  }

}
