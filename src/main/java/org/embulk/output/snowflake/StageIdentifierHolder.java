package org.embulk.output.snowflake;

import org.embulk.output.SnowflakeOutputPlugin;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class StageIdentifierHolder {
    private static final String pattern = "yyyyMMdd";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    private static final String date = simpleDateFormat.format(new Date());
    private static final String salt = SnowflakeUtils.randomString(6);
    private static final String snowflakeStageName = "embulk_snowflake_" + date + salt;
    private static final String snowflakeDestPrefix = date + "_" + salt;

    public static StageIdentifier getStageIdentifier(SnowflakeOutputPlugin.SnowflakePluginTask t){
        return new StageIdentifier(t.getDatabase(), t.getSchema(), snowflakeStageName, Optional.of(snowflakeDestPrefix));
    }
}
