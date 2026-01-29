/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.sqlserver.table.SqlServerReadableMetadata;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.data.TimestampData;

import io.debezium.data.Envelope;
import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;

/** Event deserializer for SQL Server pipeline connector. */
@Internal
public class SqlServerEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;
    private static final String TABLE_CHANGES_FIELD = "tableChanges";

    private final boolean includeSchemaChanges;
    private final List<SqlServerReadableMetadata> readableMetadataList;
    private final Map<TableId, Schema> tableSchemas;

    private final FlinkJsonTableChangeSerializer tableChangeSerializer =
            new FlinkJsonTableChangeSerializer();

    public SqlServerEventDeserializer(DebeziumChangelogMode changelogMode) {
        this(changelogMode, true, new ArrayList<>());
    }

    public SqlServerEventDeserializer(
            DebeziumChangelogMode changelogMode,
            boolean includeSchemaChanges,
            List<SqlServerReadableMetadata> readableMetadataList) {
        super(new SqlServerSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
        this.readableMetadataList = readableMetadataList;
        this.tableSchemas = new HashMap<>();
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record)
            throws Exception {
        if (!includeSchemaChanges) {
            return Collections.emptyList();
        }

        HistoryRecord historyRecord = SourceRecordUtils.getHistoryRecord(record);
        Array tableChangesArray = historyRecord.document().getArray(TABLE_CHANGES_FIELD);
        if (tableChangesArray == null || tableChangesArray.isEmpty()) {
            return Collections.emptyList();
        }

        TableChanges tableChanges = tableChangeSerializer.deserialize(tableChangesArray, true);
        List<SchemaChangeEvent> events = new ArrayList<>();
        for (TableChange tableChange : tableChanges) {
            io.debezium.relational.TableId dbzTableId = tableChange.getId();
            TableId cdcTableId = SqlServerSchemaUtils.toCdcTableId(dbzTableId);

            if (tableChange.getType() == TableChangeType.DROP) {
                events.add(new DropTableEvent(cdcTableId));
                tableSchemas.remove(cdcTableId);
                continue;
            }

            Schema newSchema = SqlServerSchemaUtils.toSchema(tableChange.getTable());
            Schema oldSchema = tableSchemas.get(cdcTableId);
            List<SchemaChangeEvent> diffEvents =
                    SchemaMergingUtils.getSchemaDifference(cdcTableId, oldSchema, newSchema);
            for (SchemaChangeEvent diffEvent : diffEvents) {
                if (diffEvent instanceof CreateTableEvent) {
                    applyChangeEvent((CreateTableEvent) diffEvent);
                }
            }
            events.addAll(diffEvents);
            tableSchemas.put(cdcTableId, newSchema);
        }

        return events;
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        return SourceRecordUtils.isSchemaChangeEvent(record);
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String databaseName = source.getString(DATABASE_NAME_KEY);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return TableId.tableId(databaseName, schemaName, tableName);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> metadataMap = new HashMap<>();
        readableMetadataList.forEach(
                sqlServerReadableMetadata -> {
                    Object metadata = sqlServerReadableMetadata.getConverter().read(record);
                    if (sqlServerReadableMetadata.equals(SqlServerReadableMetadata.OP_TS)) {
                        metadataMap.put(
                                sqlServerReadableMetadata.getKey(),
                                String.valueOf(((TimestampData) metadata).getMillisecond()));
                    } else {
                        metadataMap.put(
                                sqlServerReadableMetadata.getKey(), String.valueOf(metadata));
                    }
                });
        return metadataMap;
    }
}
