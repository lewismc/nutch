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
package org.apache.nutch.service.resources;

import java.lang.invoke.MethodHandles;
import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.nutch.service.model.response.JobInfo.State;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.LockedAccountException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.web.jaxrs.ShiroSecurityContext;
import org.apache.nutch.service.model.response.NutchServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.annotation.JacksonFeatures;

@Path(value="/admin")
public class AdminResource extends AbstractResource {

  private final int DELAY_SEC = 1;
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Get the status of the Nutch Server 
   * @return {@link NutchServerInfo} for the running service
   */
  @GET
  @Path(value="/")
  @JacksonFeatures(serializationEnable =  { SerializationFeature.INDENT_OUTPUT })
  @RequiresAuthentication
  public Response getServerStatus(@Context ShiroSecurityContext context){
    String scheme = context.getAuthenticationScheme();
    NutchServerInfo serverInfo = new NutchServerInfo();
    serverInfo.setConfiguration(this.configManager.list());
    serverInfo.setStartDate(new Date(this.server.getStarted()));
    serverInfo.setJobs(this.jobManager.list(null, State.ANY));
    serverInfo.setRunningJobs(this.jobManager.list(null, State.RUNNING));
    return Response.ok(serverInfo).build();
////    SecurityUtils.setSecurityManager(this.server.getSecurityManager());
////    Subject currentUser = SecurityUtils.getSubject();
//    // let's login the current user so we can check against roles and permissions:
//    if (!currentUser.isAuthenticated()) {
//      UsernamePasswordToken token = new UsernamePasswordToken("lonestarr", "vespa");
//      AuthenticationInfo authenticationInfo = SecurityUtils.getSecurityManager()
//              .authenticate(token);
//      token.setRememberMe(true);
//      LOG.info(authenticationInfo.toString());
//      try {
//        currentUser.login(token);
//      } catch (UnknownAccountException uae) {
//        LOG.error("There is no user with username of " + token.getPrincipal());
//      } catch (IncorrectCredentialsException ice) {
//        LOG.error("Password for account " + token.getPrincipal() + " was incorrect!");
//      } catch (LockedAccountException lae) {
//        LOG.error("The account for username " + token.getPrincipal() + " is locked but access attempts are being made.");
//      }
//      // ... catch more exceptions here (maybe custom ones specific to your application?
//      //catch (AuthenticationException ae) {
//        //unexpected condition?  error?
//      //}
//    }
//    return null;
  }

  /**
   * Stop the Nutch server
   * @param force If set to true, it will kill any running jobs
   * @return a message indicating shutdown status
   */
  @GET
  @Path(value="/stop")
  @RequiresAuthentication
  public String stopServer(@QueryParam("force") boolean force){
    if(!this.server.canStop(force)){
      return "Jobs still running -- Cannot stop server now" ;
    }    
    scheduleServerStop();
    return "Stopping in server on port " + this.server.getPort();
  }

  private void scheduleServerStop() {
    LOG.info("Shutting down server in {} sec", this.DELAY_SEC);
    Thread thread = new Thread() {
      public void run() {
        try {
          Thread.sleep(AdminResource.this.DELAY_SEC*1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        AdminResource.this.server.stop();
        LOG.info("Service stopped.");
      }
    };
    thread.setDaemon(true);
    thread.start();
    LOG.info("Service shutting down...");
  }

}
