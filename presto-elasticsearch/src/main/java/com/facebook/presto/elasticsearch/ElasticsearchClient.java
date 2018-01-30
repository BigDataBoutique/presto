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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.Sniffer;

import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient implements Closeable {

    private static final Logger log = Logger.get(ElasticsearchClient.class);

    private final ElasticsearchConnectorConfig config;
    private final RestClient restClient;
    private final Sniffer sniffer;

    private final HashMap<String, String> requestParams = newHashMap();
    private final ContentType BULK = ContentType.create("application/x-ndjson", "UTF-8");

    @Inject
    public ElasticsearchClient(final ElasticsearchConnectorConfig config)
    {
        requireNonNull(config, "config is null");
        this.config = config;

//        this.clusters.put(config.getDefaultSchema(), createClusterObject(config.getNodes()));

        // TODO support scheme (http / https) when creating the hosts
        final Set<HostAddress> nodes = config.getNodes();
        final HttpHost[] hosts =
                nodes.stream().map(add -> new HttpHost(add.getHostText(), add.getPort())).toArray(HttpHost[]::new);

        // TODO load from configs per cluster
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "changeme"));

        final RestClientBuilder restClientBuilder = RestClient.builder(hosts);

        this.restClient = restClientBuilder.build();
        this.sniffer = Sniffer.builder(this.restClient).build();
    }

    ElasticsearchClient(final ElasticsearchConnectorConfig config, RestClient restClient)
    {
        requireNonNull(config, "config is null");
        this.config = config;

        this.restClient = restClient;
        this.sniffer = Sniffer.builder(restClient).build();
    }

    public List<ColumnMetadata> getIndex(String indexAndTypeName) {
        try {
            if (indexAndTypeName.indexOf('/') == -1)
                indexAndTypeName += "/doc";
            return getIndexes().get(indexAndTypeName);
        } catch (IOException e) {
            log.error(e);
            return null;
        }
    }

    public HashMap<String, List<ColumnMetadata>> getIndexes() throws IOException {
        final Response response = restClient.performRequest("GET", "/_mapping");

        HashMap<String, List<ColumnMetadata>> indexes = newHashMap();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readValue(EntityUtils.toString(response.getEntity()), JsonNode.class);

        final Iterator<Map.Entry<String, JsonNode>> indices = rootNode.fields();
        while (indices.hasNext()) {
            Map.Entry<String, JsonNode> index = indices.next();
            if (index.getValue().get("mappings") != null) {
                final Iterator<Map.Entry<String, JsonNode>> types = index.getValue().get("mappings").fields();
                while (types.hasNext()) {
                    final Map.Entry<String, JsonNode> type = types.next();
                    if ("_default_".equals(type.getKey())) continue; // skip non-actual types

                    log.info("Discovered Elasticsearch Presto table [" + index.getKey() + "] with type " + type.getKey());

                    indexes.put(index.getKey() + "/" + type.getKey(), getColumns(type.getValue().get("properties")));
                }
            }
        }

        return indexes;
    }

    private List<ColumnMetadata> getColumns(JsonNode properties) {
        if (properties == null) {
            log.warn("Empty mapping found");
            return Collections.emptyList();
        }

        ImmutableList.Builder<ColumnMetadata> ret = ImmutableList.builder();
        Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> field = fields.next();
            final String fieldName = field.getKey();
            final JsonNode fieldDefinition = field.getValue();

            ret.add(new ColumnMetadata(fieldName, typeToPrestoType(fieldDefinition.get("type").asText())));
//            log.info("Field " + fieldName + ", def: " + fieldDefinition.toString());

            JsonNode subFields = fieldDefinition.get("fields");
            if (subFields != null) {
                Iterator<Map.Entry<String, JsonNode>> it = subFields.fields();
                while (it.hasNext()) {
                    final Map.Entry<String, JsonNode> subField = it.next();
                    ret.add(new ColumnMetadata(fieldName + "." + subField.getKey(), typeToPrestoType(subField.getValue().get("type").asText())));
//                    log.info("Field " + fieldName + "." + subField.getKey() + ", def: " + subField.getValue().toString());
                }
            }
        }
        return ret.build();
    }

    private static String prestoTypeToType(final Type prestoType) {
        if (prestoType == DOUBLE) return  "double";
        if (prestoType == TINYINT) return "short";
        if (prestoType == INTEGER) return "integer";
        if (prestoType == BIGINT) return "long";
        if (prestoType == DATE) return "date";
        if (prestoType == VARCHAR) return "text"; // TODO
        if (prestoType == BOOLEAN) return "boolean";
        if (prestoType == VARBINARY) return "binary";
        return "text";
    }

    private static Type typeToPrestoType(String type) {
        Type prestoType;
        switch (type) {
            case "double":
            case "float":
                prestoType = DOUBLE;
                break;
            case "short":
                prestoType = TINYINT;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            case "long":
                prestoType = BIGINT;
                break;
            case "date":
                prestoType = DATE;
                break;
            case "text":
            case "keyword":
            case "string":
                prestoType = VARCHAR;
                break;
            case "boolean":
                prestoType = BOOLEAN;
                break;
            case "binary":
                prestoType = VARBINARY;
                break;
            case "nested":
                // TODO unsupported?
                prestoType = VARCHAR; //JSON
                break;
            default:
                prestoType = VARCHAR; //JSON
                break;
        }
        return prestoType;
    }

    private static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private void executeQuery(final String clusterName) {
        final HttpEntity query = new NStringEntity(
                "{\n" +
                        "    \"query\" : {\n" +
                        "    \"match_all\": { } \n" +
                        "} \n"+
                        "}", ContentType.APPLICATION_JSON);
    }

    @Override
    public void close() throws IOException {
        sniffer.close();
        restClient.close();
    }

    public void createIndex(SchemaTableName table, List<ElasticsearchColumnHandle> columns) throws IOException {
        final String[] indexAndType = table.getTableName().split("/");
        final String indexName = indexAndType[0];
        final String typeName = indexAndType.length > 1 ? indexAndType[1] : "doc";

        ObjectMapper o = new ObjectMapper();
        ObjectNode idx = o.createObjectNode();
        ObjectNode settings = idx.putObject("settings");
        settings.put("index.number_of_replicas", 0);
        settings.put("index.number_of_shards", 1);

        ObjectNode mappingsProperties = idx.putObject("mappings").putObject(typeName).putObject("properties");
        for (ElasticsearchColumnHandle column : columns) {
            mappingsProperties.putObject(column.getName()).put("type", prestoTypeToType(column.getType()));
        }

        final HttpEntity indexDefinition = new NStringEntity(idx.toString(), ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest("PUT", indexName, requestParams, indexDefinition);
    }

    public void deleteIndex(SchemaTableName table) throws IOException {
        final String[] indexAndType = table.getTableName().split("/");
        restClient.performRequest("DELETE", indexAndType[0], requestParams);
    }

    public void batchIndex(SchemaTableName schemaTableName, List<ObjectNode> batch) throws IOException {
        if (batch.size() == 0) {
            return;
        }

        final String[] indexAndType = schemaTableName.getTableName().split("/");
        final String indexName = indexAndType[0];
        final String typeName = indexAndType.length > 1 ? indexAndType[1] : "doc";

        StringBuilder sb = new StringBuilder();
        for (final ObjectNode doc : batch) {
            sb.append("{\"index\":{\"_type\":\"").append(typeName).append("\"}}").append('\n');
            sb.append(doc.toString()).append('\n');
        }

        final HttpEntity bulk = new NStringEntity(sb.toString(), BULK);
        Response response = restClient.performRequest("POST", indexName + "/_bulk",
                requestParams, bulk, new BasicHeader("Content-Type", "application/x-ndjson"));
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Error while trying to write data"); // TODO
        }
    }
}
