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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordSet implements RecordSet {
    private final ElasticsearchMultiClusterClient elasticsearchClient;
    private final ElasticsearchSplit split;
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final ImmutableList<Type> columnTypes;

    public ElasticsearchRecordSet(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columnHandles, ElasticsearchMultiClusterClient elasticsearchClient) {
        this.elasticsearchClient = requireNonNull(elasticsearchClient, "client is null");
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");

        final ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (final ElasticsearchColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new ElasticsearchRecordCursor();
    }

    public class ElasticsearchRecordCursor implements RecordCursor {

        private List<Object> fields;
        private final Map<String, Integer> jsonPathToIndex;

        private long totalBytes;

        public ElasticsearchRecordCursor() {
            jsonPathToIndex = newHashMap();
            for (int i = 0; i < columnHandles.size(); i++) {
                // TODO
//                this.jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
            }
        }

//        @Override
        // TODO
        public long getTotalBytes() {
            return totalBytes;
        }

        @Override
        public long getCompletedBytes() {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos() {
            return 0;
        }

        @Override
        public Type getType(int field) {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition() {
//            if (!lines.hasNext()) {
//                return false;
//            }
//            SearchHit hit = lines.next();

            fields = newArrayList();

            // TODO this is not representing bytes used correctly
            totalBytes += fields.size();

            return true;
        }

        @Override
        public boolean getBoolean(int field) {
            checkFieldType(field, boolean.class);
            return false;
        }

        @Override
        public long getLong(int field) {
            checkFieldType(field, long.class);
            return 0;
        }

        @Override
        public double getDouble(int field) {
            checkFieldType(field, double.class);
            return 0;
        }

        @Override
        public Slice getSlice(int field) {
            return null;
        }

        @Override
        public Object getObject(int field) {
            return null;
        }

        @Override
        public boolean isNull(int field) {
            return false;
        }

        @Override
        public void close() {

        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }
    }
}
