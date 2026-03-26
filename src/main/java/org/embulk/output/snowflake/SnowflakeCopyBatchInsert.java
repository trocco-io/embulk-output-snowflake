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
  private final ExecutorService uploadExecutorService;
  private final ExecutorService copyExecutorService;
  private final StageIdentifier stageIdentifier;
  private final boolean deleteStageFile;

  protected static final String nullString = "\\N";
  protected static final String newLineString = "\n";
  protected static final String delimiterString = "\t";
  // https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
  // Number of completed uploads to accumulate before submitting a batch COPY.
  // Batching is based on upload completion order (not submission order) to avoid
  // stalling on slow uploads within a chunk.
  private static final int BATCH_COPY_CHUNK_SIZE = 20;
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
  // CompletionService wraps the upload executor to allow polling uploads by completion order.
  private final ExecutorCompletionService<String> uploadCompletionService;
  // Number of uploads submitted but not yet drained from uploadCompletionService.
  private int pendingUploads;
  // File names of completed uploads ready to be included in the next batch COPY.
  private final List<String> readyForCopyFileNames;
  // Tracks submitted batch COPY futures so finish() can wait for all pipelined COPYs.
  private final List<Future<Void>> copyFutures;
  // Accumulates all file names across chunks for stage file cleanup in finish().
  private final List<String> allUploadedFileNames;
  private boolean emptyFieldAsNull;
  private final boolean escapeWithEnclosing;

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
      boolean emptyFieldAsNull,
      boolean escapeWithEnclosing)
      throws IOException {
    this.index = 0;
    openNewFile();
    this.connector = connector;
    this.stageIdentifier = stageIdentifier;
    this.copyIntoTableColumnNames = copyIntoTableColumnNames;
    this.copyIntoCSVColumnNumbers = copyIntoCSVColumnNumbers;
    this.uploadExecutorService = Executors.newCachedThreadPool();
    // Single-thread executor for COPY: limits to 1 concurrent COPY per task to avoid
    // connection explosion, while keeping the main thread free to continue reading data.
    this.copyExecutorService = Executors.newSingleThreadExecutor();
    this.uploadCompletionService = new ExecutorCompletionService<>(uploadExecutorService);
    this.deleteStageFile = deleteStageFile;
    this.pendingUploads = 0;
    this.readyForCopyFileNames = new ArrayList<>();
    this.copyFutures = new ArrayList<>();
    this.allUploadedFileNames = new ArrayList<>();
    this.maxUploadRetries = maxUploadRetries;
    this.maxCopyRetries = maxCopyRetries;
    this.emptyFieldAsNull = emptyFieldAsNull;
    this.escapeWithEnclosing = escapeWithEnclosing;
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
    if (escapeWithEnclosing) {
      setEnclosedString(v);
    } else {
      setEscapedString(v);
    }
    nextColumn(v.length() * 2 + 4);
  }

  public void setNString(String v) throws IOException {
    appendDelimiter();
    if (escapeWithEnclosing) {
      setEnclosedString(v);
    } else {
      setEscapedString(v);
    }
    nextColumn(v.length() * 2 + 4);
  }

  public void setBytes(byte[] v) throws IOException {
    appendDelimiter();
    String s = String.valueOf(v);
    if (escapeWithEnclosing) {
      setEnclosedString(s);
    } else {
      setEscapedString(s);
    }
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
    int len = v.length();
    for (int i = 0; i < len; i++) {
      writer.write(escape(v.charAt(i)));
    }
    nextColumn(v.length() * 2 + 4);
  }

  // Enclose field with double quotes. Inside the quotes:
  // - " is escaped as "" (CSV standard)
  // - \0 (null byte) is removed
  // - All other characters (\n, \t, \r, \\) are written as-is
  private void setEnclosedString(String v) throws IOException {
    writer.write('"');
    int len = v.length();
    for (int i = 0; i < len; i++) {
      char c = v.charAt(i);
      if (c == '"') {
        writer.write("\"\"");
      } else if (c != 0) {
        writer.write(c);
      }
    }
    writer.write('"');
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
    uploadCompletionService.submit(uploadTask);
    pendingUploads++;

    fileCount++;
    totalRows += batchRows;
    batchRows = 0;
    batchWeight = 0;

    drainCompletedUploads();
    submitBatchCopyIfReady();

    openNewFile();
  }

  private void drainCompletedUploads() throws SQLException {
    Future<String> completed;
    while ((completed = uploadCompletionService.poll()) != null) {
      readyForCopyFileNames.add(getOrUnwrap(completed));
      pendingUploads--;
    }
  }

  private void submitBatchCopyIfReady() {
    while (readyForCopyFileNames.size() >= BATCH_COPY_CHUNK_SIZE) {
      List<String> batch =
          new ArrayList<>(readyForCopyFileNames.subList(0, BATCH_COPY_CHUNK_SIZE));
      readyForCopyFileNames.subList(0, BATCH_COPY_CHUNK_SIZE).clear();

      allUploadedFileNames.addAll(batch);

      copyFutures.add(
          copyExecutorService.submit(
              () -> {
                runBatchCopyWithRetry(batch);
                return null;
              }));
    }
  }

  public void close() throws IOException, SQLException {
    uploadExecutorService.shutdownNow();
    copyExecutorService.shutdownNow();

    try {
      uploadExecutorService.awaitTermination(60, TimeUnit.SECONDS);
      copyExecutorService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    closeCurrentFile().delete();
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  @Override
  public void finish() throws IOException, SQLException {
    try {
      while (pendingUploads > 0) {
        try {
          readyForCopyFileNames.add(getOrUnwrap(uploadCompletionService.take()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        pendingUploads--;
      }

      // Submit batch COPY for any full chunks among remaining completed uploads
      submitBatchCopyIfReady();

      // Run final batch COPY for remaining files (below threshold) synchronously
      if (!readyForCopyFileNames.isEmpty()) {
        allUploadedFileNames.addAll(readyForCopyFileNames);
        runBatchCopyWithRetry(readyForCopyFileNames);
      }

      for (Future<Void> future : copyFutures) {
        getOrUnwrap(future);
      }
      copyFutures.clear();

      if (!allUploadedFileNames.isEmpty()) {
        logger.info("Loaded {} files.", fileCount);
      }
    } finally {
      // Delete stage files if configured — clean up even on partial failure
      if (deleteStageFile && !allUploadedFileNames.isEmpty()) {
        deleteStageFiles(allUploadedFileNames);
      }
    }
  }

  private <T> T getOrUnwrap(Future<T> future) throws SQLException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  private void deleteStageFiles(List<String> fileNames) {
    // runDeleteStageFile appends ".csv.gz" internally, so strip it from file names
    for (String fileName : fileNames) {
      String nameWithoutExtension = fileName.replaceFirst("\\.csv\\.gz$", "");
      try {
        // Use a fresh connection per retry — the existing connection may be broken
        // after a JDBC communication error, so reusing it would fail again.
        retryWithBackoff(
            MAX_DELETE_RETRIES,
            "Delete stage file " + fileName,
            () -> {
              try (SnowflakeOutputConnection con =
                  (SnowflakeOutputConnection) connector.connect(true)) {
                con.runDeleteStageFile(stageIdentifier, nameWithoutExtension);
              }
              return null;
            });
      } catch (SQLException e) {
        logger.warn("Failed to delete stage file {}: {}", fileName, e.getMessage());
      }
    }
  }

  private void runBatchCopyWithRetry(List<String> fileNames) throws SQLException {
    retryWithBackoff(
        maxCopyRetries,
        "Batch COPY",
        () -> {
          try (SnowflakeOutputConnection con =
              (SnowflakeOutputConnection) connector.connect(true)) {
            logger.info("Running batch COPY INTO for {} files: {}", fileNames.size(), fileNames);

            long startTime = System.currentTimeMillis();
            con.runBatchCopy(
                tableIdentifier,
                stageIdentifier,
                fileNames,
                copyIntoTableColumnNames,
                copyIntoCSVColumnNumbers,
                delimiterString,
                emptyFieldAsNull,
                escapeWithEnclosing);

            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info(
                "Loaded {} files ({} seconds for batch COPY): {}",
                fileNames.size(),
                String.format("%.2f", seconds),
                fileNames);
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
        logger.warn(
            "{} error (retry {}/{}): {}", operationName, retries, maxRetries, e.getMessage());
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

  /**
   * Upload task that returns the staged file name (with .csv.gz extension) on completion. Used with
   * ExecutorCompletionService to allow batch COPY to be triggered by upload completion order.
   */
  private class UploadTask implements Callable<String> {
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

    public String call() throws SQLException {
      try {
        long startTime = System.currentTimeMillis();
        retryWithBackoff(
            maxUploadRetries,
            "Upload " + snowflakeStageFileName,
            () -> {
              try (SnowflakeOutputConnection con =
                  (SnowflakeOutputConnection) connector.connect(true)) {
                logger.info(
                    "Uploading file id {} to Snowflake ({} bytes {} rows)",
                    snowflakeStageFileName,
                    String.format("%,d", file.length()),
                    String.format("%,d", batchRows));
                FileInputStream fileInputStream = new FileInputStream(file);
                con.runUploadFile(stageIdentifier, snowflakeStageFileName, fileInputStream);
              }
              return null;
            });

        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        logger.info(
            "Uploaded file {} ({} seconds)",
            snowflakeStageFileName,
            String.format("%.2f", seconds));
      } finally {
        file.delete();
      }

      return snowflakeStageFileName + ".csv.gz";
    }
  }
}
