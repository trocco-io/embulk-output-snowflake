package org.embulk.output.snowflake;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class StageIdentifier {
    private String database;
    private String schemaName;
    private String stageName;
    private Optional<String> destPrefix;

    public StageIdentifier(String database, String schemaName, String stageName, Optional<String> destPrefix) {
        this.database = database;
        this.schemaName = schemaName;
        this.stageName = stageName;
        this.destPrefix = destPrefix;
    }

    public StageIdentifier() {
    }

    @JsonProperty
    public String getDatabase() {
        return database;
    }

    @JsonProperty
    public void setDatabase(String database) {
        this.database = database;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @JsonProperty
    public String getStageName() {
        return stageName;
    }

    @JsonProperty
    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    @JsonProperty
    public Optional<String> getDestPrefix() {
        return destPrefix;
    }

    @JsonProperty
    public void setDestPrefix(Optional<String> destPrefix) {
        this.destPrefix = destPrefix;
    }
}