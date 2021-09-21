package org.embulk.output.snowflake;

import java.util.Properties;
import org.embulk.EmbulkSystemProperties;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.SnowflakeOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.TaskMapper;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSnowflakeOutputPlugin {
  private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES =
      EmbulkSystemProperties.of(new Properties());

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .registerPlugin(OutputPlugin.class, "snowflake", SnowflakeOutputPlugin.class)
          .build();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
  private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

  private Logger logger = LoggerFactory.getLogger(TestSnowflakeOutputPlugin.class);

  public static final Properties TEST_PROPERTIES;

  static {
    Properties props = new Properties();
    props.setProperty("host", System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_HOST"));
    props.setProperty("user", System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_USER"));
    props.setProperty("password", System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_PASSWORD"));
    props.setProperty("warehouse", System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_WAREHOUSE"));
    props.setProperty("db", System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_DATABASE"));
    props.setProperty("schema", System.getenv("EMBULK_OUTPUT_SNOWFLAKE_TEST_SCHEMA"));
    TEST_PROPERTIES = props;
  }
}
