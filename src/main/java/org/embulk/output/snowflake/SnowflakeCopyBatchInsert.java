package org.embulk.output.snowflake;

import java.io.*;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
  private final int maxUploadRetries;

  private SnowflakeOutputConnection connection = null;
  private TableIdentifier tableIdentifier = null;
  protected File currentFile;
  protected BufferedWriter writer;
  protected int index;
  protected int batchRows;
  private int batchWeight;
  private long totalRows;
  private int fileCount;
  private List<Future<Void>> uploadAndCopyFutures;
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
      boolean emptyFieldAsNull)
      throws IOException {
    this.index = 0;
    openNewFile();
    this.connector = connector;
    this.stageIdentifier = stageIdentifier;
    this.copyIntoTableColumnNames = copyIntoTableColumnNames;
    this.copyIntoCSVColumnNumbers = copyIntoCSVColumnNumbers;
    this.executorService = new MemoryAwareCachedThreadPoolExecutor();
    this.deleteStageFile = deleteStageFile;
    this.uploadAndCopyFutures = new ArrayList();
    this.maxUploadRetries = maxUploadRetries;
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
    Path path = newFile.toPath();

    // Snowflake supports gzip
    return new BufferedWriter(
        new OutputStreamWriter(
            new GZIPOutputStream(new BufferedOutputStream(Files.newOutputStream(path))),
            FILE_CHARSET));
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

    String snowflakeStageFileName = "embulk_snowflake_" + SnowflakeUtils.randomString(8);

    UploadTask uploadTask =
        new UploadTask(file, batchRows, stageIdentifier, snowflakeStageFileName, maxUploadRetries);
    Future<Void> uploadFuture = executorService.submit(uploadTask);
    uploadAndCopyFutures.add(uploadFuture);

    CopyTask copyTask = new CopyTask(uploadFuture, snowflakeStageFileName, emptyFieldAsNull);
    uploadAndCopyFutures.add(executorService.submit(copyTask));

    fileCount++;
    totalRows += batchRows;
    batchRows = 0;
    batchWeight = 0;

    openNewFile();
  }

  public void close() throws IOException, SQLException {
    executorService.shutdown();

    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          logger.error("ExecutorService did not terminate");
        }
      }
    } catch (InterruptedException ie) {
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
    for (Future<Void> uploadAndCopyFuture : uploadAndCopyFutures) {
      try {
        uploadAndCopyFuture.get();

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof SQLException) {
          throw (SQLException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }

    logger.info("Loaded {} files.", fileCount);
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

    public Void call() throws IOException, SQLException, InterruptedException {
      int retries = 0;
      long startTime = System.currentTimeMillis();
      Path path = file.toPath();
      // put file to snowflake internal storage

      while (true) {
        try (SnowflakeOutputConnection con = (SnowflakeOutputConnection) connector.connect(true);
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
            BufferedInputStream bufferedInputStream =
                new BufferedInputStream(Channels.newInputStream(fileChannel))) {
          logger.info(
              String.format(
                  "Uploading file id %s to Snowflake (%,d bytes %,d rows)",
                  snowflakeStageFileName, file.length(), batchRows));
          con.runUploadFile(stageIdentifier, snowflakeStageFileName, bufferedInputStream);
          break;
        } catch (SQLException e) {
          retries++;
          if (retries > this.maxUploadRetries) {
            throw e;
          }
          logger.warn(
              String.format(
                  "Upload error %s file %s retries: %d", e, snowflakeStageFileName, retries));
          Thread.sleep(retries * retries * 1000);
        }
      }

      double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

      logger.info(
          String.format("Uploaded file %s (%.2f seconds)", snowflakeStageFileName, seconds));

      file.delete();

      return null;
    }

    @Override
    public String toString() {
      return String.format(
          "UploadTask[file=%s, batchRows=%d, stageIdentifier=%s, snowflakeStageFileName=%s, maxUploadRetries=%d]",
          file, batchRows, stageIdentifier, snowflakeStageFileName, maxUploadRetries);
    }
  }

  private class CopyTask implements Callable<Void> {
    private final Future<Void> uploadFuture;
    private final String snowflakeStageFileName;
    private final boolean emptyFieldAsNull;

    public CopyTask(
        Future<Void> uploadFuture, String snowflakeStageFileName, boolean emptyFieldAsNull) {
      this.uploadFuture = uploadFuture;
      this.snowflakeStageFileName = snowflakeStageFileName;
      this.emptyFieldAsNull = emptyFieldAsNull;
    }

    public Void call() throws SQLException, InterruptedException, ExecutionException {
      try {
        uploadFuture.get();

        SnowflakeOutputConnection con = (SnowflakeOutputConnection) connector.connect(true);
        try {
          logger.info("Running COPY from file {}", snowflakeStageFileName);

          long startTime = System.currentTimeMillis();
          con.runCopy(
              tableIdentifier,
              stageIdentifier,
              snowflakeStageFileName,
              copyIntoTableColumnNames,
              copyIntoCSVColumnNumbers,
              delimiterString,
              emptyFieldAsNull);

          double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

          logger.info(
              String.format(
                  "Loaded file %s (%.2f seconds for COPY)", snowflakeStageFileName, seconds));

        } finally {
          con.close();
        }
      } finally {
        if (deleteStageFile) {
          connection.runDeleteStageFile(stageIdentifier, snowflakeStageFileName);
        }
      }

      return null;
    }

    @Override
    public String toString() {
      return String.format(
          "CopyTask[uploadFuture=%s, snowflakeStageFileName=%s, emptyFieldAsNull=%s]",
          uploadFuture, snowflakeStageFileName, emptyFieldAsNull);
    }
  }
}
