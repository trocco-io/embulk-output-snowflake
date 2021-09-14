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

  private SnowflakeOutputConnection connection = null;
  private TableIdentifier tableIdentifier = null;
  protected File currentFile;
  protected BufferedWriter writer;
  protected int index;
  protected int batchRows;
  private long totalRows;
  private int fileCount;
  private List<Future<Void>> uploadAndCopyFutures;

  public SnowflakeCopyBatchInsert(
      JdbcOutputConnector connector, StageIdentifier stageIdentifier, boolean deleteStageFile)
      throws IOException {
    this.index = 0;
    openNewFile();
    this.connector = connector;
    this.stageIdentifier = stageIdentifier;
    this.executorService = Executors.newCachedThreadPool();
    this.deleteStageFile = deleteStageFile;
    this.uploadAndCopyFutures = new ArrayList();
  }

  @Override
  public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException {
    this.connection = (SnowflakeOutputConnection) connector.connect(true);
    this.connection.runCreateStage(stageIdentifier);
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
    long fsize = currentFile.length();
    if (fsize > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) fsize;
    }
  }

  public void add() throws IOException {
    writer.write(newLineString);
    batchRows++;
    index = 0;
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
  }

  public void setBoolean(boolean v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setByte(byte v) throws IOException {
    appendDelimiter();
    setEscapedString(String.valueOf(v));
  }

  public void setShort(short v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setInt(int v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setLong(long v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setFloat(float v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setDouble(double v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setBigDecimal(BigDecimal v) throws IOException {
    appendDelimiter();
    writer.write(String.valueOf(v));
  }

  public void setString(String v) throws IOException {
    appendDelimiter();
    setEscapedString(v);
  }

  public void setNString(String v) throws IOException {
    appendDelimiter();
    setEscapedString(v);
  }

  public void setBytes(byte[] v) throws IOException {
    appendDelimiter();
    setEscapedString(String.valueOf(v));
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
  }

  private void setEscapedString(String v) throws IOException {
    for (char c : v.toCharArray()) {
      writer.write(escape(c));
    }
  }

  @Override
  public void flush() throws IOException, SQLException {
    File file = closeCurrentFile(); // flush buffered data in writer

    String snowflakeStageFileName = "embulk_snowflake_" + SnowflakeUtils.randomString(8);

    UploadTask uploadTask =
        new UploadTask(file, batchRows, stageIdentifier, snowflakeStageFileName);
    Future<Void> uploadFuture = executorService.submit(uploadTask);
    uploadAndCopyFutures.add(uploadFuture);

    CopyTask copyTask = new CopyTask(uploadFuture, snowflakeStageFileName);
    uploadAndCopyFutures.add(executorService.submit(copyTask));

    fileCount++;
    totalRows += batchRows;
    batchRows = 0;

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

    public UploadTask(
        File file, int batchRows, StageIdentifier stageIdentifier, String snowflakeStageFileName) {
      this.file = file;
      this.batchRows = batchRows;
      this.snowflakeStageFileName = snowflakeStageFileName;
      this.stageIdentifier = stageIdentifier;
    }

    public Void call() throws IOException, SQLException {
      logger.info(
          String.format(
              "Uploading file id %s to Snowflake (%,d bytes %,d rows)",
              snowflakeStageFileName, file.length(), batchRows));

      try {
        long startTime = System.currentTimeMillis();
        // put file to snowflake internal storage
        SnowflakeOutputConnection con = (SnowflakeOutputConnection) connector.connect(true);

        FileInputStream fileInputStream = new FileInputStream(file);
        con.runUploadFile(stageIdentifier, snowflakeStageFileName, fileInputStream);

        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

        logger.info(
            String.format("Uploaded file %s (%.2f seconds)", snowflakeStageFileName, seconds));
      } finally {
        file.delete();
      }

      return null;
    }
  }

  private class CopyTask implements Callable<Void> {
    private final Future<Void> uploadFuture;
    private final String snowflakeStageFileName;

    public CopyTask(Future<Void> uploadFuture, String snowflakeStageFileName) {
      this.uploadFuture = uploadFuture;
      this.snowflakeStageFileName = snowflakeStageFileName;
    }

    public Void call() throws SQLException, InterruptedException, ExecutionException {
      try {
        uploadFuture.get();

        SnowflakeOutputConnection con = (SnowflakeOutputConnection) connector.connect(true);
        try {
          logger.info("Running COPY from file {}", snowflakeStageFileName);

          long startTime = System.currentTimeMillis();
          con.runCopy(tableIdentifier, stageIdentifier, snowflakeStageFileName, delimiterString);

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
  }
}
