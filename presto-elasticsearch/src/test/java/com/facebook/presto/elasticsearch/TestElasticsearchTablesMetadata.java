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
import com.facebook.presto.spi.type.VarcharType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class TestElasticsearchTablesMetadata extends BaseElasticsearchConnectorTestCase {

    public void testBasicIndex() throws IOException {
        index("test", "doc", "1", "{ \"foo\": \"bar\" }");
        refresh("test");

        final ElasticsearchConnectorConfig config = new ElasticsearchConnectorConfig();
        config.setNodes("http://localhost:51010");
        try (ElasticsearchClient client = new ElasticsearchClient(config, getRestClient())) {
            HashMap<String, List<ColumnMetadata>> indexes = client.getIndexes();

            assertEquals(1, indexes.size());

            List<ColumnMetadata> columns = indexes.get("test/doc");
            assertEquals(2, columns.size());
            assertEquals("foo", columns.get(0).getName());
            assertEquals(VarcharType.VARCHAR, columns.get(0).getType());
            assertEquals("foo.keyword", columns.get(1).getName());
            assertEquals(VarcharType.VARCHAR, columns.get(1).getType());
        }
    }
}
