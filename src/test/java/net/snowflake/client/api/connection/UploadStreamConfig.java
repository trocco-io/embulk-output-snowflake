package net.snowflake.client.api.connection;

/**
 * Test stub of the Snowflake JDBC 4.x {@code UploadStreamConfig}, with the same package, name and
 * public signatures as the real one (checked against 4.4.1). {@code compressData} defaults to
 * {@code true}, as in the real class. Keep it in sync with the real API.
 */
public class UploadStreamConfig {
  private final String destPrefix;
  private final boolean compressData;

  private UploadStreamConfig(Builder builder) {
    this.destPrefix = builder.destPrefix;
    this.compressData = builder.compressData;
  }

  public String getDestPrefix() {
    return destPrefix;
  }

  public boolean isCompressData() {
    return compressData;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String destPrefix;
    private boolean compressData = true;

    public Builder setDestPrefix(String destPrefix) {
      this.destPrefix = destPrefix;
      return this;
    }

    public Builder setCompressData(boolean compressData) {
      this.compressData = compressData;
      return this;
    }

    public UploadStreamConfig build() {
      return new UploadStreamConfig(this);
    }
  }
}
