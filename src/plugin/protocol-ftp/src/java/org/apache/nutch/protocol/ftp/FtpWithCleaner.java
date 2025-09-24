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
import java.lang.ref.Cleaner;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.io.IOException;

/**
 * A modern implementation of the FTP protocol plugin that uses the Cleaner API
 * (Java 9+) to handle resource cleanup without the deprecated finalize() method.
 * 
 * This class is a protocol plugin used for ftp: scheme. It creates
 * {@link FtpResponse} object and gets the content of the url from it.
 * Configurable parameters are {@code ftp.username}, {@code ftp.password},
 * {@code ftp.content.limit}, {@code ftp.timeout}, {@code ftp.server.timeout},
 * {@code ftp.password}, {@code ftp.keep.connection} and {@code ftp.follow.talk}.
 * For details see "FTP properties" section in {@code nutch-default.xml}.
 * 
 * The Cleaner API provides a more reliable and performant alternative to
 * finalization for automatic resource management.
 * 
 * @author Apache Nutch Team
 * @since Nutch 1.20
 */
public class FtpWithCleaner implements Protocol {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int BUFFER_SIZE = 16384; // 16*1024 = 16384
  private static final Cleaner cleaner = Cleaner.create();

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
  volatile Client client = null;
  // ftp dir list entry parser
  FTPFileEntryParser parser = null;

  private Configuration conf;
  private FtpRobotRulesParser robots = null;
  private final Cleaner.Cleanable cleanable;
  private final CleanupState state;

  /**
   * State object that holds the resources to be cleaned up.
   * This class must not hold references to the outer FtpWithCleaner instance.
   */
  private static class CleanupState implements Runnable {
    private volatile Client clientRef;
    private volatile boolean cleaned = false;

    void setClient(Client client) {
      this.clientRef = client;
    }

    @Override
    public void run() {
      if (!cleaned) {
        try {
          if (clientRef != null && clientRef.isConnected()) {
            try {
              clientRef.logout();
            } catch (IOException e) {
              LOG.debug("Error during FTP logout in cleaner: {}", e.getMessage());
            }
            try {
              clientRef.disconnect();
            } catch (IOException e) {
              LOG.debug("Error during FTP disconnect in cleaner: {}", e.getMessage());
            }
          }
        } finally {
          cleaned = true;
          clientRef = null;
        }
      }
    }
  }

  // constructor
  public FtpWithCleaner() {
    robots = new FtpRobotRulesParser();
    this.state = new CleanupState();
    this.cleanable = cleaner.register(this, state);
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
        
        // Update the client reference in the cleanup state
        if (this.client != null) {
          state.setClient(this.client);
        }

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
   * Explicitly close and clean up the FTP connection resources.
   * This method provides a way to manually trigger cleanup.
   */
  public void close() {
    // Trigger the cleanup immediately
    cleanable.clean();
  }

  /**
   * Explicitly disconnect the FTP client.
   * This method can be called to immediately release FTP resources.
   */
  public void disconnect() {
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
      state.setClient(null);
    }
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

    String usage = "Usage: FtpWithCleaner [-logLevel level] [-followTalk] [-keepConnection] [-timeout N] [-maxContentLength L] [-dumpContent] url";

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

    FtpWithCleaner ftp = new FtpWithCleaner();
    try {
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
    } finally {
      // Explicitly close to ensure cleanup
      ftp.close();
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