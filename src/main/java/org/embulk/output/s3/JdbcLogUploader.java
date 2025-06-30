package org.embulk.output.s3;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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

  public JdbcLogUploader(
      String bucket, String prefix, String region, String accessKeyId, String secretAccessKey) {
    this.bucket = bucket;
    this.prefix = prefix;
    this.region = Region.of(region);

    if (accessKeyId != null && secretAccessKey != null) {
      // Use explicit credentials
      AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
      this.s3Client =
          S3Client.builder()
              .region(this.region)
              .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
              .build();
    } else {
      // Use default credentials provider (IAM role, environment variables, etc.)
      this.s3Client =
          S3Client.builder()
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

    // Add timestamp to filename
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    String originalFileName = file.getName();
    String fileNameWithTimestamp;

    // Insert timestamp before file extension
    int lastDotIndex = originalFileName.lastIndexOf('.');
    if (lastDotIndex > 0) {
      String nameWithoutExt = originalFileName.substring(0, lastDotIndex);
      String extension = originalFileName.substring(lastDotIndex);
      fileNameWithTimestamp = nameWithoutExt + "_" + timestamp + extension;
    } else {
      fileNameWithTimestamp = originalFileName + "_" + timestamp;
    }

    String key = prefix.isEmpty() ? fileNameWithTimestamp : prefix + "/" + fileNameWithTimestamp;
    try {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(bucket).key(key).build();

      s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
      logger.info("Uploaded {}", file.getAbsolutePath());
    } catch (Exception e) {
      logger.error("Failed to upload {}", file.getAbsolutePath(), e);
    }
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }
}
