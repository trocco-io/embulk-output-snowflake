package org.embulk.output.snowflake;

import static org.junit.Assert.assertEquals;

import java.io.*;
import java.util.zip.GZIPInputStream;
import org.junit.Test;

public class TestSnowflakeCopyBatchInsert {

  private SnowflakeCopyBatchInsert createBatchInsert(boolean escapeWithEnclosing) throws Exception {
    return new SnowflakeCopyBatchInsert(
        null, null, new String[0], new int[0], false, 3, 3, true, escapeWithEnclosing);
  }

  private String readGzipFile(File file) throws Exception {
    try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(file));
        InputStreamReader reader = new InputStreamReader(gis, "UTF-8");
        BufferedReader br = new BufferedReader(reader)) {
      StringBuilder sb = new StringBuilder();
      char[] buf = new char[1024];
      int len;
      while ((len = br.read(buf)) != -1) {
        sb.append(buf, 0, len);
      }
      return sb.toString();
    }
  }

  // escape() tests (existing backslash escape behavior)

  @Test
  public void testEscapeBackslash() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("\\\\", batch.escape('\\'));
    batch.close();
  }

  @Test
  public void testEscapeNewline() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("\\n", batch.escape('\n'));
    batch.close();
  }

  @Test
  public void testEscapeTab() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("\\t", batch.escape('\t'));
    batch.close();
  }

  @Test
  public void testEscapeCarriageReturn() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("\\r", batch.escape('\r'));
    batch.close();
  }

  @Test
  public void testEscapeNullByte() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("", batch.escape('\0'));
    batch.close();
  }

  @Test
  public void testEscapeRegularChar() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("a", batch.escape('a'));
    batch.close();
  }

  @Test
  public void testEscapeDoubleQuoteNotEscaped() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    assertEquals("\"", batch.escape('"'));
    batch.close();
  }

  // setString() without enclosing (existing behavior)

  @Test
  public void testSetStringWithoutEnclosing() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    batch.setString("hello\tworld\n");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("hello\\tworld\\n\n", content);
    file.delete();
  }

  // setString() with enclosing (new behavior)

  @Test
  public void testSetStringWithEnclosing() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("hello\tworld\n");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"hello\tworld\n\"\n", content);
    file.delete();
  }

  @Test
  public void testEnclosingEscapesDoubleQuotes() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("say \"hello\"");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"say \"\"hello\"\"\"\n", content);
    file.delete();
  }

  @Test
  public void testEnclosingRemovesNullByte() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("hello\0world");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"helloworld\"\n", content);
    file.delete();
  }

  @Test
  public void testEnclosingPreservesBackslash() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("path\\to\\file");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"path\\to\\file\"\n", content);
    file.delete();
  }

  @Test
  public void testEnclosingPreservesCarriageReturn() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("line1\r\nline2");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"line1\r\nline2\"\n", content);
    file.delete();
  }

  @Test
  public void testMultipleColumnsWithEnclosing() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("col1\nvalue");
    batch.setString("col2\tvalue");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"col1\nvalue\"\t\"col2\tvalue\"\n", content);
    file.delete();
  }

  @Test
  public void testMultipleColumnsWithoutEnclosing() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(false);
    batch.setString("col1\nvalue");
    batch.setString("col2\tvalue");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("col1\\nvalue\tcol2\\tvalue\n", content);
    file.delete();
  }

  @Test
  public void testEmptyStringWithEnclosing() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setString("");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("\"\"\n", content);
    file.delete();
  }

  @Test
  public void testNonStringColumnsNotEnclosed() throws Exception {
    SnowflakeCopyBatchInsert batch = createBatchInsert(true);
    batch.setBoolean(true);
    batch.setLong(42L);
    batch.setString("text\n");
    batch.add();

    File file = batch.currentFile;
    batch.writer.close();

    String content = readGzipFile(file);
    assertEquals("true\t42\t\"text\n\"\n", content);
    file.delete();
  }
}
