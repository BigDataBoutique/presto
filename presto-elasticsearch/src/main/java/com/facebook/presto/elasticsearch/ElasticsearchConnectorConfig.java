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

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class ElasticsearchConnectorConfig {
    private static final int ES_DEFAULT_PORT = 9200;

    /**
     * Seed nodes for Elasticsearch cluster. At least one must exist.
     */
    private Set<HostAddress> nodes = ImmutableSet.of();

    /**
     * Timeout to connect to Elasticsearch.
     */
    private Duration elasticsearchConnectTimeout = Duration.valueOf("10s");

    /**
     * Timeout to read data from Elasticsearch for a single query.
     */
    private Duration clientReadTimeout = Duration.valueOf("1m");

    /**
     * The default name of the Elasticsearch cluster we are accessing, aka Presto Schema
     */
    private String defaultClusterName = "es";

    /**
     * The minimum number of docs allowed for a split (for when queries return a small number of hits,
     * and there are many workers available)
     */
    private int minimumNumberOfDocsPerSplit = 10000;

    /**
     * The maximum number of docs allowed for a split (unbounded by default)
     */
    private int maximumNumberOfDocsPerSplit = -1;

    @NotNull
    public String getDefaultSchema()
    {
        return defaultClusterName;
    }

    @Config("elasticsearch.default-cluster-name")
    public ElasticsearchConnectorConfig setDefaultSchema(String defaultClusterName)
    {
        this.defaultClusterName = defaultClusterName;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("elasticsearch.nodes")
    public ElasticsearchConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    // TODO authentication support

    @MinDuration("1s")
    public Duration getKafkaConnectTimeout()
    {
        return elasticsearchConnectTimeout;
    }

    @Config("elasticsearch.connect-timeout")
    public ElasticsearchConnectorConfig setKafkaConnectTimeout(String esConnectTimeout)
    {
        this.elasticsearchConnectTimeout = Duration.valueOf(esConnectTimeout);
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientReadTimeout()
    {
        return clientReadTimeout;
    }

    @Config("elasticsearch.client.read-timeout")
    public ElasticsearchConnectorConfig setClientReadTimeout(Duration clientReadTimeout)
    {
        this.clientReadTimeout = clientReadTimeout;
        return this;
    }

    @Min(100)
    public int getMinimumNumberOfDocsPerSplit()
    {
        return minimumNumberOfDocsPerSplit;
    }

    @Config("elasticsearch.min-docs-per-split")
    public ElasticsearchConnectorConfig setMinimumNumberOfDocsPerSplit(int minimumNumberOfDocsPerSplit)
    {
        this.minimumNumberOfDocsPerSplit = minimumNumberOfDocsPerSplit;
        return this;
    }

    public int getMaximumNumberOfDocsPerSplit()
    {
        return maximumNumberOfDocsPerSplit;
    }

    @Config("elasticsearch.max-docs-per-split")
    public ElasticsearchConnectorConfig setMaximumNumberOfDocsPerSplit(int maximumNumberOfDocsPerSplit)
    {
        this.maximumNumberOfDocsPerSplit = maximumNumberOfDocsPerSplit;
        return this;
    }

    static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        // TODO currently http / https protocol parsing is not supported
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), ElasticsearchConnectorConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(ES_DEFAULT_PORT);
    }
}
