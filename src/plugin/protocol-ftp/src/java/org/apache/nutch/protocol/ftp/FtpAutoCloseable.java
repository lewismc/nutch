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
package org.apache.nutch.protocol.ftp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.net.ftp.FTPFileEntryParser;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.nutch.net.protocols.Response;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import crawlercommons.robots.BaseRobotRules;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.io.IOException;

/**
 * A modern implementation of the FTP protocol plugin that implements AutoCloseable
 * to handle resource cleanup without using the deprecated finalize() method.
 * 
 * This class is a protocol plugin used for ftp: scheme. It creates
 * {@link FtpResponse} object and gets the content of the url from it.
 * Configurable parameters are {@code ftp.username}, {@code ftp.password},
 * {@code ftp.content.limit}, {@code ftp.timeout}, {@code ftp.server.timeout},
 * {@code ftp.password}, {@code ftp.keep.connection} and {@code ftp.follow.talk}.
 * For details see "FTP properties" section in {@code nutch-default.xml}.
 * 
 * This implementation provides automatic resource cleanup through the
 * AutoCloseable interface, which should be used with try-with-resources blocks
 * or explicit close() calls.
 * 
 * @author Apache Nutch Team
 * @since Nutch 1.20
 */
public class FtpAutoCloseable implements Protocol, AutoCloseable {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int BUFFER_SIZE = 16384; // 16*1024 = 16384

  static final int MAX_REDIRECTS = 5;

  int timeout;
  int maxContentLength;
  String userName;
  String passWord;

  // typical/default server timeout is 120*1000 millisec.
  // better be conservative here
  int serverTimeout;

  // when to have client start anew
  long renewalTime = -1;

  boolean keepConnection;
  boolean followTalk;

  // ftp client
  Client client = null;
  // ftp dir list entry parser
  FTPFileEntryParser parser = null;

  private Configuration conf;
  private FtpRobotRulesParser robots = null;
  private volatile boolean closed = false;

  // constructor
  public FtpAutoCloseable() {
    robots = new FtpRobotRulesParser();
  }

  /**
   * Set the timeout.
   * @param to a maximum timeout in milliseconds
   */
  public void setTimeout(int to) {
    timeout = to;
  }

  /**
   * Set the length after at which content is truncated.
   * @param length max content length in bytes
   */
  public void setMaxContentLength(int length) {
    maxContentLength = length;
  }

  /**
   * Set followTalk i.e. to log dialogue between our client and remote
   * server. Useful for debugging.
   * @param followTalk if true will follow, false by default
   */
  public void setFollowTalk(boolean followTalk) {
    this.followTalk = followTalk;
  }

  /**
   * Whether to keep ftp connection. Useful if crawling same host
   * again and again. When set to true, it avoids connection, login and dir list
   * parser setup for subsequent URLs. If it is set to true, however, you must
   * make sure (roughly):
   * (1) ftp.timeout is less than ftp.server.timeout
   * (2) ftp.timeout is larger than (fetcher.threads.fetch * fetcher.server.delay)
   * Otherwise there will be too many "delete client because idled too long"
   * messages in thread logs.
   * @param keepConnection if true we will keep the connection, false by default
   */
  public void setKeepConnection(boolean keepConnection) {
    this.keepConnection = keepConnection;
  }

  /**
   * Creates a {@link FtpResponse} object corresponding to the url and returns a
   * {@link ProtocolOutput} object as per the content received
   * 
   * @param url
   *          Text containing the ftp url
   * @param datum
   *          The CrawlDatum object corresponding to the url
   * 
   * @return {@link ProtocolOutput} object for the url
   */
  @Override
  public ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum) {
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);

      int redirects = 0;

      while (true) {
        FtpResponse response;
        response = new FtpResponse(u, datum, this, getConf()); // make a request

        int code = response.getCode();
        datum.getMetaData().put(Nutch.PROTOCOL_STATUS_CODE_KEY,
          new Text(Integer.toString(code)));
        

        if (code == 200) { // got a good response
          return new ProtocolOutput(response.toContent()); // return it

        } else if (code >= 300 && code < 400) { // handle redirect
          if (redirects == MAX_REDIRECTS)
            throw new FtpException("Too many redirects: " + url);
          
          String loc = response.getHeader("Location");
          try {
            u = new URL(u, loc);
          } catch (MalformedURLException mue) {
            LOG.error("Could not create redirectURL for {} with {}", url, loc);
            return new ProtocolOutput(null, new ProtocolStatus(mue));
          }
          
          redirects++;
          if (LOG.isTraceEnabled()) {
            LOG.trace("redirect to " + u);
          }
        } else { // convert to exception
          throw new FtpError(code);
        }
      }
    } catch (Exception e) {
      LOG.error("Could not get protocol output for {}: {}", url,
          e.getMessage());
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }

  /**
   * Implements the AutoCloseable interface to provide automatic resource cleanup.
   * This method replaces the deprecated finalize() method.
   * 
   * When used with try-with-resources, this method will be called automatically
   * to clean up FTP connection resources.
   * 
   * @throws Exception if an error occurs during cleanup
   */
  @Override
  public void close() throws Exception {
    if (!closed) {
      try {
        cleanupFtpConnection();
      } finally {
        closed = true;
      }
    }
  }

  /**
   * Clean up FTP connection resources.
   * This method performs the actual cleanup of FTP client connections.
   */
  private void cleanupFtpConnection() {
    if (this.client != null && this.client.isConnected()) {
      try {
        this.client.logout();
      } catch (IOException e) {
        LOG.debug("Error during FTP logout: {}", e.getMessage());
      }
      try {
        this.client.disconnect();
      } catch (IOException e) {
        LOG.debug("Error during FTP disconnect: {}", e.getMessage());
      }
      this.client = null;
    }
  }

  /**
   * Check if the FTP connection has been closed
   * @return true if the connection has been closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Explicitly disconnect the FTP client.
   * This method can be called to immediately release FTP resources.
   */
  public void disconnect() {
    cleanupFtpConnection();
  }

  /** 
   * For debugging.
   * @param args run with no args for help
   * @throws Exception if there is an error running this program
   */
  public static void main(String[] args) throws Exception {
    int timeout = Integer.MIN_VALUE;
    int maxContentLength = Integer.MIN_VALUE;
    @SuppressWarnings("unused")
    String logLevel = "info";
    boolean followTalk = false;
    boolean keepConnection = false;
    boolean dumpContent = false;
    String urlString = null;

    String usage = "Usage: FtpAutoCloseable [-logLevel level] [-followTalk] [-keepConnection] [-timeout N] [-maxContentLength L] [-dumpContent] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-logLevel")) {
        logLevel = args[++i];
      } else if (args[i].equals("-followTalk")) {
        followTalk = true;
      } else if (args[i].equals("-keepConnection")) {
        keepConnection = true;
      } else if (args[i].equals("-timeout")) {
        timeout = Integer.parseInt(args[++i]) * 1000;
      } else if (args[i].equals("-maxContentLength")) {
        maxContentLength = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-dumpContent")) {
        dumpContent = true;
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else {
        urlString = args[i];
      }
    }

    // Use try-with-resources for automatic cleanup
    try (FtpAutoCloseable ftp = new FtpAutoCloseable()) {
      ftp.setFollowTalk(followTalk);
      ftp.setKeepConnection(keepConnection);

      if (timeout != Integer.MIN_VALUE) // set timeout
        ftp.setTimeout(timeout);

      if (maxContentLength != Integer.MIN_VALUE) // set maxContentLength
        ftp.setMaxContentLength(maxContentLength);

      Content content = ftp.getProtocolOutput(new Text(urlString),
          new CrawlDatum()).getContent();

      System.err.println("Content-Type: " + content.getContentType());
      System.err.println("Content-Length: "
          + content.getMetadata().get(Response.CONTENT_LENGTH));
      System.err.println("Last-Modified: "
          + content.getMetadata().get(Response.LAST_MODIFIED));
      if (dumpContent) {
        System.out.print(new String(content.getContent()));
      }
    }
  }

  /**
   * Set the {@link Configuration} object
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxContentLength = conf.getInt("ftp.content.limit", 1024 * 1024);
    this.timeout = conf.getInt("ftp.timeout", 10000);
    this.userName = conf.get("ftp.username", "anonymous");
    this.passWord = conf.get("ftp.password", "anonymous@example.com");
    this.serverTimeout = conf.getInt("ftp.server.timeout", 60 * 1000);
    this.keepConnection = conf.getBoolean("ftp.keep.connection", false);
    this.followTalk = conf.getBoolean("ftp.follow.talk", false);
    this.robots.setConf(conf);
  }

  /**
   * Get the {@link Configuration} object
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Get the robots rules for a given url
   */
  @Override
  public BaseRobotRules getRobotRules(Text url, CrawlDatum datum,
      List<Content> robotsTxtContent) {
    return robots.getRobotRulesSet(this, url, robotsTxtContent);
  }

  public int getBufferSize() {
    return BUFFER_SIZE;
  }
}