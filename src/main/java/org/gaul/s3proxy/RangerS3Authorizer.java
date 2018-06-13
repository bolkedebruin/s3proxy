package org.gaul.s3proxy;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.READ_ACCCESS_TYPE;
import static org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants.WRITE_ACCCESS_TYPE;

public class RangerS3Authorizer implements S3Authorizer {
  public static final String KEY_RESOURCE_PATH = "path";

  private static final Logger logger = LoggerFactory.getLogger(
    RangerS3Authorizer.class);

  private RangerS3Plugin rangerPlugin = null;
  private Map<String, String> access2actionMapper = new HashMap<String, String>();

  public RangerS3Authorizer() {
    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerS3Authorizer.RangerS3Authorizer()");
    }

    start();

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerS3Authorizer.RangerS3Authorizer()");
    }

  }

  public void start() {
    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerS3Authorizer.start()");
    }

    RangerS3Plugin plugin = new RangerS3Plugin();
    plugin.init();

    access2actionMapper.put("GET", READ_ACCCESS_TYPE);
    access2actionMapper.put("DELETE", WRITE_ACCCESS_TYPE);
    access2actionMapper.put("HEAD", READ_ACCCESS_TYPE);
    access2actionMapper.put("POST", WRITE_ACCCESS_TYPE);
    access2actionMapper.put("PUT", WRITE_ACCCESS_TYPE);

    rangerPlugin = plugin;

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerS3Authorizer.start()");
    }
  }

  /*
  public void stop() {
    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerS3Authorizer.stop()");
    }

    RangerS3Plugin plugin = rangerPlugin;
    rangerPlugin = null;

    if (plugin != null) {
      plugin.cleanup();
    }

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerS3Authorizer.stop()");
    }

  }
  */

  @Override
  public boolean isAccessAllowed(String path, String method, String requestIdentity,
                                 String clientIp) {
    RangerS3AccessRequest request = new RangerS3AccessRequest(
      path,
      method,
      access2actionMapper.get(method),
      requestIdentity,
      getUserGroups(requestIdentity),
      clientIp
    );

    RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
    boolean accessAllowed = result != null && result.getIsAllowed();

    if (logger.isDebugEnabled()) {
      logger.debug(String.format("accessAllowed=%s method=%s for " +
        "user=%s to resource=%s with privileges=%s",
        accessAllowed, method, requestIdentity, path, access2actionMapper.get(method)));
    }

    return accessAllowed;
  }

  /**
   * Returns a set of groups a user belongs to
   * @param requestIdentity identity of the user
   * @return set of groups for the user
   */
  private Set<String> getUserGroups(String requestIdentity) {
    String[] userGroups = null;
    try {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(requestIdentity);
      userGroups = ugi.getGroupNames();
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("Determined user=%s belongs to groups=%s",
          requestIdentity, Arrays.toString(userGroups)));
      }
    } catch (Throwable e) {
      logger.warn(String.format("Unable to determine groups for user=%s",
        requestIdentity));
    }

    return userGroups == null ? Collections.<String>emptySet() :
      Sets.<String>newHashSet(Arrays.asList(userGroups));

  }
}

class RangerS3AccessRequest extends RangerAccessRequestImpl {

  public RangerS3AccessRequest(String path, String method, String accessType,
                               String requestIdentity, Set<String> groups,
                               String clientIp) {
    super.setResource(new RangerS3Resource(path, requestIdentity));
    super.setAccessType(accessType);
    super.setUser(requestIdentity);
    super.setUserGroups(groups);
    super.setAccessTime(new Date());
    super.setAction(method);
    super.setClientIPAddress(clientIp);
  }
}

class RangerS3Resource extends RangerAccessResourceImpl {
  public RangerS3Resource(String path, String ownerUser) {
    super.setValue(RangerS3Authorizer.KEY_RESOURCE_PATH, path);
    super.setOwnerUser(ownerUser);
  }
}

class RangerS3Plugin extends RangerBasePlugin {
  public RangerS3Plugin() {
    super("s3", "s3");
  }

  public void init() {
    super.init();

  }
}