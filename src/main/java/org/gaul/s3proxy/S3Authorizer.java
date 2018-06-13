package org.gaul.s3proxy;

public interface S3Authorizer {
  boolean isAccessAllowed(String path, String method, String requestIdentity, String clientIp);
}
