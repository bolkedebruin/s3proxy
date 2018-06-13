package org.gaul.s3proxy;

import org.apache.ranger.plugin.client.BaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.model.User;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class S3Client extends BaseClient {
  private String endpoint;
  private String accesskey;
  private String secretkey;
  private String uid;

  private static final Logger logger = LoggerFactory.getLogger(
    S3Client.class);

  public S3Client(String serviceName, Map<String, String> configs) {
    super(serviceName, configs, "s3-client");

    this.endpoint = configs.get("endpoint");
    this.accesskey = configs.get("accesskey");
    this.secretkey = configs.get("secretkey");
    this.uid = configs.get("uid");

    if (this.endpoint == null || this.endpoint.isEmpty()) {
      logger.error("No value found for configuration `endpoint`. Lookup will fail");
    }

    if (this.accesskey == null || this.accesskey.isEmpty()) {
      logger.error("No value found for configuration `key`. Lookup will fail");
    }

    if (this.secretkey == null || this.secretkey.isEmpty()) {
      logger.error("No value found for configuration `token`. Lookup will fail");
    }

    if (this.uid == null || this.uid.isEmpty()) {
      logger.error("No value found for configuration `token`. Lookup will fail");
    }
  }

  public List<String> getBuckets(final String userInput) {
    RgwAdmin rgwAdmin = new RgwAdminBuilder()
      .accessKey(this.accesskey)
      .secretKey(this.secretkey)
      .endpoint(this.endpoint)
      .build();

    return rgwAdmin.listBucket(this.uid);
  }

  public Map<String, Object> connectionTest() {
    Map<String, Object> responseData = new HashMap<String, Object>();

    RgwAdmin rgwAdmin = new RgwAdminBuilder()
      .accessKey(this.accesskey)
      .secretKey(this.secretkey)
      .endpoint(this.endpoint)
      .build();

    Optional<User> user = rgwAdmin.getUserInfo(this.uid);

    if (!user.isPresent()) {
      final String errMessage = "Cannot connect to S3 endpoint (or radosgw)";
      BaseClient.generateResponseDataMap(false, errMessage, errMessage,
        null, null, responseData);
    } else {
      final String successMessage = "Connection test succesful";
      BaseClient.generateResponseDataMap(true, successMessage, successMessage,
        null, null, responseData);
    }

    return responseData;
  }
}
