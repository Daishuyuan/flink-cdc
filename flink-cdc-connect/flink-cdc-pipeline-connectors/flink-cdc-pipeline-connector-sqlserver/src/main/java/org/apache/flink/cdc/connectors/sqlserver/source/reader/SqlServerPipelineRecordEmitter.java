/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.sqlserver.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** The {@link RecordEmitter} implementation for SQL Server pipeline connector. */
public class SqlServerPipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    private final SqlServerSourceConfig sourceConfig;
    private Set<TableId> alreadySendCreateTableTables;

    private boolean shouldEmitAllCreateTableEventsInSnapshotMode = true;
    private boolean isBounded = false;

    private final Map<TableId, CreateTableEvent> createTableEventCache;

    public SqlServerPipelineRecordEmitter(
            DebeziumDeserializationSchema debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            SqlServerSourceConfig sourceConfig,
            OffsetFactory offsetFactory) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
        this.sourceConfig = sourceConfig;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache =
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .getCreateTableEventCache();
        generateCreateTableEvent(sourceConfig);
        this.isBounded = StartupOptions.snapshot().equals(sourceConfig.getStartupOptions());
    }

    @Override
    public void applySplit(SourceSplitBase split) {
        if ((isBounded) && createTableEventCache.isEmpty() && split instanceof SnapshotSplit) {
            createTableEventCache.putAll(generateCreateTableEvent(sourceConfig));
        } else {
            for (TableChanges.TableChange tableChange : split.getTableSchemas().values()) {
                CreateTableEvent createTableEvent =
                        new CreateTableEvent(
                                SqlServerSchemaUtils.toCdcTableId(tableChange.getId()),
                                SqlServerSchemaUtils.toSchema(tableChange.getTable()));
                ((DebeziumEventDeserializationSchema) debeziumDeserializationSchema)
                        .applyChangeEvent(createTableEvent);
            }
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (shouldEmitAllCreateTableEventsInSnapshotMode && isBounded) {
            createTableEventCache.forEach(
                    (tableId, createTableEvent) -> output.collect((T) createTableEvent));
            shouldEmitAllCreateTableEventsInSnapshotMode = false;
        } else if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                sendCreateTableEvent(tableId, (SourceOutput<Event>) output);
                alreadySendCreateTableTables.add(tableId);
            }
        } else {
            boolean isDataChange = isDataChangeRecord(element);
            if (isDataChange || isSchemaChangeEvent(element)) {
                TableId tableId = getTableId(element);
                if (!alreadySendCreateTableTables.contains(tableId)) {
                    CreateTableEvent createTableEvent = createTableEventCache.get(tableId);
                    if (createTableEvent != null) {
                        output.collect((T) createTableEvent);
                    }
                    alreadySendCreateTableTables.add(tableId);
                }
                if (isDataChange && !createTableEventCache.containsKey(tableId)) {
                    CreateTableEvent createTableEvent = getCreateTableEvent(tableId);
                    output.collect((T) createTableEvent);
                    createTableEventCache.put(tableId, createTableEvent);
                }
            }
        }
        super.processElement(element, output, splitState);
    }

    private CreateTableEvent getCreateTableEvent(TableId tableId) {
        org.apache.flink.cdc.common.event.TableId cdcTableId =
                SqlServerSchemaUtils.toCdcTableId(tableId);
        Schema schema = SqlServerSchemaUtils.getTableSchema(sourceConfig, cdcTableId);
        return new CreateTableEvent(cdcTableId, schema);
    }

    private void sendCreateTableEvent(TableId tableId, SourceOutput<Event> output) {
        output.collect(getCreateTableEvent(tableId));
    }

    private Map<TableId, CreateTableEvent> generateCreateTableEvent(
            SqlServerSourceConfig sourceConfig) {
        List<org.apache.flink.cdc.common.event.TableId> capturedTableIds =
                SqlServerSchemaUtils.listTables(sourceConfig);
        Map<TableId, CreateTableEvent> createTableEventCache = new HashMap<>();
        for (org.apache.flink.cdc.common.event.TableId tableId : capturedTableIds) {
            Schema schema = SqlServerSchemaUtils.getTableSchema(sourceConfig, tableId);
            createTableEventCache.put(
                    SqlServerSchemaUtils.toDbzTableId(tableId),
                    new CreateTableEvent(tableId, schema));
        }
        return createTableEventCache;
    }
}
