package org.embulk.output.snowflake;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPOutputStream;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeCopyBatchInsert implements BatchInsert {
  private final Logger logger = LoggerFactory.getLogger(SnowflakeCopyBatchInsert.class);
  private final JdbcOutputConnector connector;
  protected static final Charset FILE_CHARSET = Charset.forName("UTF-8");
  private final ExecutorService executorService;
  private final StageIdentifier stageIdentifier;
  private final boolean deleteStageFile;

  protected static final String nullString = "\\N";
  protected static final String newLineString = "\n";
  protected static final String delimiterString = "\t";
  // https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
  // "The maximum number of files names that can be specified is 1000."
  private static final int MAX_FILES_PER_COPY = 1000;
  private static final int MAX_DELETE_RETRIES = 3;
  private final int maxUploadRetries;
  private final int maxCopyRetries;

  private SnowflakeOutputConnection connection = null;
  private TableIdentifier tableIdentifier = null;
  protected File currentFile;
  protected BufferedWriter writer;
  protected int index;
  protected int batchRows;
  private int batchWeight;
  private long totalRows;
  private int fileCount;
  private List<Future<Void>> uploadFutures;
  private List<String> uploadedFileNames;
  private boolean emptyFieldAsNull;

  private String[] copyIntoTableColumnNames;

  private int[] copyIntoCSVColumnNumbers;

  public SnowflakeCopyBatchInsert(
      JdbcOutputConnector connector,
      StageIdentifier stageIdentifier,
      String[] copyIntoTableColumnNames,
      int[] copyIntoCSVColumnNumbers,
      boolean deleteStageFile,
      int maxUploadRetries,
      int maxCopyRetries,
      boolean emptyFieldAsNull)
      throws IOException {
    this.index = 0;
    openNewFile();
    this.connector = connector;
    this.stageIdentifier = stageIdentifier;
    this.copyIntoTableColumnNames = copyIntoTableColumnNames;
    this.copyIntoCSVColumnNumbers = copyIntoCSVColumnNumbers;
    this.executorService = Executors.newCachedThreadPool();
    this.deleteStageFile = deleteStageFile;
    this.uploadFutures = new ArrayList<>();
    this.uploadedFileNames = Collections.synchronizedList(new ArrayList<>());
    this.maxUploadRetries = maxUploadRetries;
    this.maxCopyRetries = maxCopyRetries;
    this.emptyFieldAsNull = emptyFieldAsNull;
  }

  @Override
  public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException {
    this.connection = (SnowflakeOutputConnection) connector.connect(true);
    this.tableIdentifier = loadTable;
  }

  private File createTempFile() throws IOException {
    return File.createTempFile(
        "embulk-output-snowflake-copy-", ".tsv.tmp"); // TODO configurable temporary file path
  }

  protected File openNewFile() throws IOException {
    File newFile = createTempFile();
    File oldFile = closeCurrentFile();
    this.writer = openWriter(newFile);
    currentFile = newFile;
    return oldFile;
  }

  protected File closeCurrentFile() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
    return currentFile;
  }

  protected BufferedWriter openWriter(File newFile) throws IOException {
    // Snowflake supports gzip
    return new BufferedWriter(
        new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(newFile)), FILE_CHARSET));
  }

  public int getBatchWeight() {
    return batchWeight;
  }

  public void add() throws IOException {
    writer.write(newLineString);
    batchRows++;
    index = 0;
    batchWeight += 32;
  }

  private void appendDelimiter() throws IOException {
    if (index != 0) {
      writer.write(delimiterString);
    }
    index++;
  }

  public void setNull(int sqlType) throws IOException {
    appendDelimiter();
    writer.write(nullString);
    nextColumn(0);
  }

  public void setBoolean(boolean v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn(1);
  }

  public void setByte(byte v) throws IOException {
    appendDelimiter();
    setEscapedString(String.valueOf(v));
    nextColumn(1);
  }

  public void setShort(short v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn(2);
  }

  public void setInt(int v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn(4);
  }

  public void setLong(long v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn(8);
  }

  public void setFloat(float v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn(4);
  }

  public void setDouble(double v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn(8);
  }

  public void setBigDecimal(BigDecimal v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
    nextColumn((v.precision() & ~2) / 2 + 8);
  }

  public void setString(String v) throws IOException {
    appendDelimiter();
    setEscapedString(v);
    nextColumn(v.length() * 2 + 4);
  }

  public void setNString(String v) throws IOException {
    appendDelimiter();
    setEscapedString(v);
    nextColumn(v.length() * 2 + 4);
  }

  public void setBytes(byte[] v) throws IOException {
    appendDelimiter();
    setEscapedString(String.valueOf(v));
    nextColumn(v.length + 4);
  }

  @Override
  public void setSqlDate(final Instant v, final Calendar cal) throws IOException {
    appendDelimiter();
    cal.setTimeInMillis(v.getEpochSecond() * 1000);
    String f =
        String.format(
            Locale.ENGLISH,
            "%04d-%02d-%02d",
            cal.get(Calendar.YEAR),
            cal.get(Calendar.MONTH) + 1,
            cal.get(Calendar.DAY_OF_MONTH));
    writer.write(f);
    nextColumn(32);
  }

  @Override
  public void setSqlTime(final Instant v, final Calendar cal) throws IOException {
    appendDelimiter();
    cal.setTimeInMillis(v.getEpochSecond() * 1000);
    String f =
        String.format(
            Locale.ENGLISH,
            "%02d:%02d:%02d.%06d",
            cal.get(Calendar.HOUR_OF_DAY),
            cal.get(Calendar.MINUTE),
            cal.get(Calendar.SECOND),
            v.getNano() / 1000);
    writer.write(f);
    nextColumn(32);
  }

  private void nextColumn(int weight) {
    batchWeight += weight + 4; // add weight as overhead of each columns
  }

  @Override
  public void setSqlTimestamp(final Instant v, final Calendar cal) throws IOException {
    appendDelimiter();
    cal.setTimeInMillis(v.getEpochSecond() * 1000);
    int zoneOffset =
        cal.get(Calendar.ZONE_OFFSET) / 1000 / 60; // zone offset considering DST in minute
    String offset;
    if (zoneOffset >= 0) {
      offset = String.format(Locale.ENGLISH, "+%02d%02d", zoneOffset / 60, zoneOffset % 60);
    } else {
      offset = String.format(Locale.ENGLISH, "-%02d%02d", -zoneOffset / 60, -zoneOffset % 60);
    }
    String f =
        String.format(
            Locale.ENGLISH,
            "%d-%02d-%02d %02d:%02d:%02d.%06d%s",
            cal.get(Calendar.YEAR),
            cal.get(Calendar.MONTH) + 1,
            cal.get(Calendar.DAY_OF_MONTH),
            cal.get(Calendar.HOUR_OF_DAY),
            cal.get(Calendar.MINUTE),
            cal.get(Calendar.SECOND),
            v.getNano() / 1000,
            offset);
    writer.write(f);
    nextColumn(32);
  }

  private void setEscapedString(String v) throws IOException {
    for (char c : v.toCharArray()) {
      writer.write(escape(c));
    }
    nextColumn(v.length() * 2 + 4);
  }

  @Override
  public void flush() throws IOException, SQLException {
    File file = closeCurrentFile(); // flush buffered data in writer

    if (batchRows == 0) {
      logger.info("Skipping upload of empty file");
      file.delete();
      openNewFile();
      return;
    }

    String snowflakeStageFileName = "embulk_snowflake_" + SnowflakeUtils.randomString(8);

    UploadTask uploadTask =
        new UploadTask(file, batchRows, stageIdentifier, snowflakeStageFileName, maxUploadRetries);
    uploadFutures.add(executorService.submit(uploadTask));
    uploadedFileNames.add(snowflakeStageFileName + ".csv.gz");

    fileCount++;
    totalRows += batchRows;
    batchRows = 0;
    batchWeight = 0;

    openNewFile();
  }

  public void close() throws IOException, SQLException {
    executorService.shutdownNow();

    try {
      executorService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }

    closeCurrentFile().delete();
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  @Override
  public void finish() throws IOException, SQLException {
    // Wait for all uploads to complete
    for (Future<Void> uploadFuture : uploadFutures) {
      try {
        uploadFuture.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof SQLException) {
          throw (SQLException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }

    if (uploadedFileNames.isEmpty()) {
      return;
    }

    // Run batch COPY in chunks of MAX_FILES_PER_COPY
    try {
      for (int i = 0; i < uploadedFileNames.size(); i += MAX_FILES_PER_COPY) {
        List<String> chunk =
            uploadedFileNames.subList(
                i, Math.min(i + MAX_FILES_PER_COPY, uploadedFileNames.size()));
        runBatchCopyWithRetry(chunk);
      }

      logger.info("Loaded {} files.", fileCount);
    } finally {
      // Delete stage files if configured — clean up even on partial failure
      // runDeleteStageFile appends ".csv.gz" internally, so strip it from uploadedFileNames
      if (deleteStageFile) {
        for (String fileName : uploadedFileNames) {
          String nameWithoutExtension = fileName.replaceFirst("\\.csv\\.gz$", "");
          try {
            retryWithBackoff(MAX_DELETE_RETRIES, "Delete stage file " + fileName, () -> {
              connection.runDeleteStageFile(stageIdentifier, nameWithoutExtension);
              return null;
            });
          } catch (SQLException e) {
            logger.warn("Failed to delete stage file {}: {}", fileName, e.getMessage());
          }
        }
      }
    }
  }

  private void runBatchCopyWithRetry(List<String> fileNames) throws SQLException {
    retryWithBackoff(maxCopyRetries, "Batch COPY", () -> {
      try (SnowflakeOutputConnection con = (SnowflakeOutputConnection) connector.connect(true)) {
        logger.info("Running batch COPY INTO for {} files: {}", fileNames.size(), fileNames);

        long startTime = System.currentTimeMillis();
        con.runBatchCopy(
            tableIdentifier,
            stageIdentifier,
            fileNames,
            copyIntoTableColumnNames,
            copyIntoCSVColumnNumbers,
            delimiterString,
            emptyFieldAsNull);

        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        logger.info("Loaded {} files ({} seconds for batch COPY): {}",
            fileNames.size(), String.format("%.2f", seconds), fileNames);
      }
      return null;
    });
  }

  @FunctionalInterface
  private interface RetryableOperation<T> {
    T execute() throws SQLException, IOException, InterruptedException;
  }

  private <T> T retryWithBackoff(
      int maxRetries, String operationName, RetryableOperation<T> operation) throws SQLException {
    int retries = 0;
    while (true) {
      try {
        return operation.execute();
      } catch (SQLException e) {
        if (!isRetryable(e)) {
          throw e;
        }
        retries++;
        if (retries > maxRetries) {
          throw e;
        }
        logger.warn("{} error (retry {}/{}): {}", operationName, retries, maxRetries, e.getMessage());
        try {
          Thread.sleep(retries * retries * 1000);
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private boolean isRetryable(SQLException e) {
    String message = e.getMessage();
    return message != null && message.contains("JDBC driver encountered communication error");
  }

  @Override
  public int[] getLastUpdateCounts() {
    // need not be implemented because SnowflakeCopyBatchInsert won't retry.
    return new int[] {};
  }

  // Escape \, \n, \t, \r
  // Remove \0
  protected String escape(char c) {
    switch (c) {
      case '\\':
        return "\\\\";
      case '\n':
        return "\\n";
      case '\t':
        return "\\t";
      case '\r':
        return "\\r";
      case 0:
        return "";
      default:
        return String.valueOf(c);
    }
  }

  private class UploadTask implements Callable<Void> {
    private final File file;
    private final int batchRows;
    private final String snowflakeStageFileName;
    private final StageIdentifier stageIdentifier;
    private final int maxUploadRetries;

    public UploadTask(
        File file,
        int batchRows,
        StageIdentifier stageIdentifier,
        String snowflakeStageFileName,
        int maxUploadRetries) {
      this.file = file;
      this.batchRows = batchRows;
      this.snowflakeStageFileName = snowflakeStageFileName;
      this.stageIdentifier = stageIdentifier;
      this.maxUploadRetries = maxUploadRetries;
    }

    public Void call() throws SQLException {
      try {
        long startTime = System.currentTimeMillis();
        retryWithBackoff(maxUploadRetries, "Upload " + snowflakeStageFileName, () -> {
          try (SnowflakeOutputConnection con =
              (SnowflakeOutputConnection) connector.connect(true)) {
            logger.info("Uploading file id {} to Snowflake ({} bytes {} rows)",
                snowflakeStageFileName, String.format("%,d", file.length()),
                String.format("%,d", batchRows));
            FileInputStream fileInputStream = new FileInputStream(file);
            con.runUploadFile(stageIdentifier, snowflakeStageFileName, fileInputStream);
          }
          return null;
        });

        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        logger.info("Uploaded file {} ({} seconds)",
            snowflakeStageFileName, String.format("%.2f", seconds));
      } finally {
        file.delete();
      }

      return null;
    }
  }
}
