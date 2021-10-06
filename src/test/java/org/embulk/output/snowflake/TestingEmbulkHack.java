package org.embulk.output.snowflake;

import java.lang.reflect.Field;
import org.embulk.EmbulkEmbed;
import org.embulk.test.TestingEmbulk;

public class TestingEmbulkHack {

  public static EmbulkEmbed getEmbulkEmbed(TestingEmbulk embulk) {
    try {
      Class obj = embulk.getClass();
      Field field = obj.getDeclaredField("embed");
      field.setAccessible(true);
      return field.get(embulk) == null ? null : (EmbulkEmbed) field.get(embulk);
    } catch (IllegalArgumentException
        | IllegalAccessException
        | NoSuchFieldException
        | SecurityException e) {
      throw new RuntimeException(e);
    }
  }
}
