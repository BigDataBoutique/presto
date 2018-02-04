/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.elasticsearch.ElasticsearchPlugin.checkType;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ElasticsearchMetadata implements ConnectorMetadata {

    private static final Logger log = Logger.get(ElasticsearchMetadata.class);

    private final Map<String, Map<String, Object>> schemas = new HashMap<>();

    private final String connectorId;
    private final ElasticsearchClient elasticsearchClient;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    @Inject
    public ElasticsearchMetadata(ElasticsearchConnectorId connectorId,
                                 ElasticsearchClient elasticsearchClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "client is null");

        this.schemas.put("default", new HashMap<>());
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        // TODO hack
        return true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        // TODO schemas are essentially ES clusters, and should maybe be taken from the config
//        return ImmutableList.copyOf(elasticsearchClient.getClusterNames());
        return ImmutableList.copyOf(schemas.keySet());
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        requireNonNull(tableName, "tableName is null");

        List<ColumnMetadata> table = elasticsearchClient.getIndex(tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ElasticsearchTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        ElasticsearchTableHandle handle = checkType(table, ElasticsearchTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ElasticsearchTableHandle elasticsearchTableHandle = checkType(tableHandle, ElasticsearchTableHandle.class, "table");
        checkArgument(elasticsearchTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(elasticsearchTableHandle.getSchemaName(), elasticsearchTableHandle.getTableName());
        return getTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        // TODO support prefixed indexes ("time based indexes", "rolling indexes") as a single partitioned table
        final ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        try {
            for (final String tableName : elasticsearchClient.getIndexes().keySet()) {
                tableNames.add(new SchemaTableName("default", tableName));
            }
        } catch (IOException e) {
            log.error(e, "Error while trying to list Elasticsearch tables");
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ElasticsearchTableHandle elasticsearchTableHandle = checkType(tableHandle, ElasticsearchTableHandle.class, "tableHandle");
        checkArgument(elasticsearchTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        List<ColumnMetadata> table = elasticsearchClient.getIndex(elasticsearchTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(elasticsearchTableHandle.toSchemaTableName());
        }

        int index = 0;
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata column : table) {
            columnHandles.put(
                    column.getName(),
                    new ElasticsearchColumnHandle(
//                            connectorId,
                            column.getName(),
                            column.getType(),
                            false
//                            esColumn.getJsonPath(),
//                            esColumn.getJsonType(),
//                            index
                    ));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((ElasticsearchColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        List<ColumnMetadata> columns = elasticsearchClient.getIndex(tableName.getTableName());
        if (columns == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, columns);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    private static List<ElasticsearchColumnHandle> buildColumnHandles(ConnectorTableMetadata tableMetadata)
    {
        return tableMetadata.getColumns().stream()
                .map(m -> new ElasticsearchColumnHandle(m.getName(), m.getType(), m.isHidden()))
                .collect(toList());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
        List<ElasticsearchColumnHandle> columns = buildColumnHandles(tableMetadata);
        try {
            if (elasticsearchClient.getIndex(tableMetadata.getTable().getTableName()) != null) {
                if (ignoreExisting) {
                    elasticsearchClient.deleteIndex(tableMetadata.getTable());
                } else {
                    throw new PrestoException(ALREADY_EXISTS, "Index [" + tableMetadata.getTable() + "] already exists on Elasticsearch");
                }
            }
            elasticsearchClient.createIndex(tableMetadata.getTable(), columns);
        } catch (IOException e) {
            log.error(e, "Error while trying to create a table on Elasticsearch");
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout)
    {
        List<ElasticsearchColumnHandle> columns = buildColumnHandles(tableMetadata);

        try {
            elasticsearchClient.createIndex(tableMetadata.getTable(), columns);
        } catch (IOException e) {
            log.error(e, "Error while trying to create a table on Elasticsearch");
            return null;
        }

        setRollback(() -> {
            try {
                elasticsearchClient.deleteIndex(tableMetadata.getTable());
            } catch (IOException e) {
                log.warn(e, "Error while trying to rollback index creation");
            }
        });

        return new ElasticsearchOutputTableHandle(
                tableMetadata.getTable(),
                columns.stream().filter(c -> !c.isHidden()).collect(toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        clearRollback();
        return Optional.empty();
    }
}
