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

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.elasticsearch.ElasticsearchPlugin.checkType;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager implements ConnectorSplitManager {

    private final String connectorId;
    private final NodeManager nodeManager;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchConnectorId connectorId, NodeManager nodeManager) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {
        // The best split strategy for Elasticsearch is to use the Slice Scroll API to have workers execute their own queries
        // See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#sliced-scroll
        // Based on total number of hits for a query, we try to make splits based on number of nodes to distribute as
        // much of the work as possible, as long as the number of documents in a single slice isn't too small (10,000
        // docs by default; configurable).

        ElasticsearchTableLayoutHandle layoutHandle = checkType(layout, ElasticsearchTableLayoutHandle.class, "partition");
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();


        // TODO execute query to get total number of hits?

        // TODO number of shards - keeping the number of slices <= number of shards avoids in-shard slicing which
        // TODO significantly slows down first queries

        // TODO possibly use field data for slicing, and also index selection (partitoining)

//        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
//        nodes.size()

        List<ConnectorSplit> splits = new ArrayList<>(1);
        splits.add(new ElasticsearchSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), layoutHandle.getTupleDomain()));
        return new FixedSplitSource(splits);
    }
}
