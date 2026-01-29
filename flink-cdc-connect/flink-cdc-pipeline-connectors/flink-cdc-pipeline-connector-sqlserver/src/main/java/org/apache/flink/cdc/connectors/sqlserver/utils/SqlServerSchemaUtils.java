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

package org.apache.flink.cdc.connectors.sqlserver.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class SqlServerSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSchemaUtils.class);
    private static final ConcurrentMap<String, SqlServerDialect> DIALECT_CACHE =
            new ConcurrentHashMap<>();

    private SqlServerSchemaUtils() {}

    public static List<TableId> listTables(SqlServerSourceConfig sourceConfig) {
        try (SqlServerConnection jdbc = getSqlServerDialect(sourceConfig).openJdbcConnection()) {
            return getSqlServerDialect(sourceConfig)
                    .discoverDataCollections(sourceConfig)
                    .stream()
                    .map(SqlServerSchemaUtils::toCdcTableId)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Error to list tables: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(SqlServerSourceConfig sourceConfig, TableId tableId) {
        try (SqlServerConnection jdbc = getSqlServerDialect(sourceConfig).openJdbcConnection()) {
            io.debezium.relational.TableId dbzTableId = toDbzTableId(tableId);
            TableChange tableChange =
                    getSqlServerDialect(sourceConfig).queryTableSchema(jdbc, dbzTableId);
            return toSchema(tableChange.getTable());
        } catch (Exception e) {
            throw new RuntimeException("Error to get table schema: " + e.getMessage(), e);
        }
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(SqlServerSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        if (column.defaultValueExpression().isPresent()) {
            return Column.physicalColumn(
                    column.name(),
                    SqlServerTypeUtils.fromDbzColumn(column),
                    column.comment(),
                    column.defaultValueExpression().get());
        }
        return Column.physicalColumn(
                column.name(), SqlServerTypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getNamespace(), tableId.getSchemaName(), tableId.getTableName());
    }

    public static TableId toCdcTableId(io.debezium.relational.TableId dbzTableId) {
        return TableId.tableId(dbzTableId.catalog(), dbzTableId.schema(), dbzTableId.table());
    }

    private static SqlServerDialect getSqlServerDialect(SqlServerSourceConfig sourceConfig) {
        String key =
                String.format(
                        "%s:%s:%s",
                        sourceConfig.getHostname(),
                        sourceConfig.getPort(),
                        sourceConfig.getDatabaseList().get(0));
        return DIALECT_CACHE.computeIfAbsent(key, k -> new SqlServerDialect(sourceConfig));
    }
}
