package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ElasticsearchInsertTableHandle implements ConnectorInsertTableHandle {
    private final SchemaTableName schemaTableName;
    private final List<ElasticsearchColumnHandle> columns;

    @JsonCreator
    public ElasticsearchInsertTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columns") List<ElasticsearchColumnHandle> columns)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<ElasticsearchColumnHandle> getColumns()
    {
        return columns;
    }

}
