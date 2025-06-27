package org.embulk.output.s3;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class JdbcLogUploader implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(JdbcLogUploader.class);

  private final S3Client s3Client;
  private final String bucket;
  private final String prefix;
  private final Region region;

  public JdbcLogUploader(String bucket, String prefix, String region, String accessKeyId, String secretAccessKey) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.region = Region.of(region);
    
    if (accessKeyId != null && secretAccessKey != null) {
      // Use explicit credentials
      AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
      this.s3Client = S3Client.builder()
        .region(this.region)
        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
        .build();
    } else {
      // Use default credentials provider (IAM role, environment variables, etc.)
      this.s3Client = S3Client.builder()
        .region(this.region)
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build();
    }
  }

  public void uploadIfExists(File file) {
    if (!file.exists()) {
      logger.warn("File not found: {}", file.getAbsolutePath());
      return;
    }

    String key = prefix + "/" + file.getName();
    try {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(bucket).key(key).build();

      s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
      logger.info("Uploaded {} to s3://{}/{}", file.getAbsolutePath(), bucket, key);
    } catch (Exception e) {
      logger.error("Failed to upload {} to S3: {}", file.getAbsolutePath(), e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }
}
