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

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ElasticsearchPageSink
        implements ConnectorPageSink {

    private ElasticsearchClient clusterClient;
    private SchemaTableName schemaTableName;
    private List<ElasticsearchColumnHandle> columns;

    private final ObjectMapper mapper = new ObjectMapper();
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset

    public ElasticsearchPageSink(ElasticsearchClient clusterClient,
                                 SchemaTableName schemaTableName,
                                 List<ElasticsearchColumnHandle> columns)
    {
        this.clusterClient = clusterClient;
        this.schemaTableName = schemaTableName;
        this.columns = columns;

        TimeZone tz = TimeZone.getTimeZone("UTC");
        dateFormat.setTimeZone(tz);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        final List<ObjectNode> batch = new ArrayList<>(page.getPositionCount());

        for (int position = 0; position < page.getPositionCount(); position++) {
            final ObjectNode doc = mapper.createObjectNode();

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                ElasticsearchColumnHandle column = columns.get(channel);
                addDocumentField(doc, column.getName(), columns.get(channel).getType(), page.getBlock(channel), position);
            }
            batch.add(doc);
        }

        try {
            clusterClient.batchIndex(schemaTableName, batch);
        } catch (IOException e) {
            e.printStackTrace(); // TODO
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {

    }

//    private boolean isImplicitRowType(Type type)
//    {
//        return type.getTypeSignature().getParameters()
//                .stream()
//                .map(TypeSignatureParameter::getNamedTypeSignature)
//                .map(NamedTypeSignature::getName)
//                .allMatch(name -> name.startsWith(implicitPrefix));
//    }

    private void addDocumentField(ObjectNode doc, String name, Type type, Block block, int position)
    {
//        if (block.isNull(position)) {
//            if (type.equals(OBJECT_ID)) {
//                return new ObjectId();
//            }
//            return null;
//        }
//
//        if (type.equals(OBJECT_ID)) {
//            return new ObjectId(block.getSlice(position, 0, block.getSliceLength(position)).getBytes());
//        }
        if (type.equals(BooleanType.BOOLEAN)) {
            doc.put(name, type.getBoolean(block, position));
        }
        else if (type.equals(BigintType.BIGINT)) {
            doc.put(name, type.getLong(block, position));
        }
        else if (type.equals(IntegerType.INTEGER)) {
            doc.put(name, (int) type.getLong(block, position));
        }
        else if (type.equals(SmallintType.SMALLINT)) {
            doc.put(name, (short) type.getLong(block, position));
        }
        else if (type.equals(TinyintType.TINYINT)) {
            doc.put(name, (byte) type.getLong(block, position));
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            doc.put(name, type.getDouble(block, position));
        }
        else if (isVarcharType(type)) {
            doc.put(name, type.getSlice(block, position).toStringUtf8());
        }
        else if (type.equals(DateType.DATE)) {
            long days = type.getLong(block, position);
            doc.put(name, dateFormat.format(new Date(TimeUnit.DAYS.toMillis(days))));
        }
        else if (type.equals(TimeType.TIME)) {
            long millisUtc = type.getLong(block, position);
            doc.put(name, dateFormat.format(new Date(millisUtc)));
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            long millisUtc = type.getLong(block, position);
            doc.put(name, dateFormat.format(new Date(millisUtc)));
        }
        else if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            long millisUtc = unpackMillisUtc(type.getLong(block, position));
            doc.put(name, dateFormat.format(new Date(millisUtc)));
        }
        else if (type instanceof DecimalType) {
            // TODO: decimal type might not support yet
            // TODO: this code is likely wrong and should switch to Decimals.readBigDecimal()
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaledValue;
            if (decimalType.isShort()) {
                unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
            }
            else {
                unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
            }
            doc.put(name, new BigDecimal(unscaledValue));
        }
//        else if (isArrayType(type)) {
//            Type elementType = type.getTypeParameters().get(0);
//
//            Block arrayBlock = block.getObject(position, Block.class);
//
//            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
//            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
//                Object element = getObjectValue(elementType, arrayBlock, i);
//                list.add(element);
//            }
//
//            return unmodifiableList(list);
//        }
//        else if (isMapType(type)) {
//            Type keyType = type.getTypeParameters().get(0);
//            Type valueType = type.getTypeParameters().get(1);
//
//            Block mapBlock = block.getObject(position, Block.class);
//
//            // map type is converted into list of fixed keys document
//            List<Object> values = new ArrayList<>(mapBlock.getPositionCount() / 2);
//            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
//                Map<String, Object> mapValue = new HashMap<>();
//                mapValue.put("key", getObjectValue(keyType, mapBlock, i));
//                mapValue.put("value", getObjectValue(valueType, mapBlock, i + 1));
//                values.add(mapValue);
//            }
//
//            return unmodifiableList(values);
//        }
//        else if (isRowType(type)) {
//            Block rowBlock = block.getObject(position, Block.class);
//
//            List<Type> fieldTypes = type.getTypeParameters();
//            if (fieldTypes.size() != rowBlock.getPositionCount()) {
//                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
//            }
//
//            if (isImplicitRowType(type)) {
//                List<Object> rowValue = new ArrayList<>();
//                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
//                    Object element = getObjectValue(fieldTypes.get(i), rowBlock, i);
//                    rowValue.add(element);
//                }
//                return unmodifiableList(rowValue);
//            }
//
//            Map<String, Object> rowValue = new HashMap<>();
//            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
//                rowValue.put(
//                        type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName(),
//                        getObjectValue(fieldTypes.get(i), rowBlock, i));
//            }
//            return unmodifiableMap(rowValue);
//        }

        else
            throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    static boolean isRowType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    static boolean isDateType(Type type)
    {
        return type.equals(DATE) ||
                type.equals(TIME) ||
                type.equals(TIMESTAMP) ||
                type.equals(TIMESTAMP_WITH_TIME_ZONE);
    }
}
