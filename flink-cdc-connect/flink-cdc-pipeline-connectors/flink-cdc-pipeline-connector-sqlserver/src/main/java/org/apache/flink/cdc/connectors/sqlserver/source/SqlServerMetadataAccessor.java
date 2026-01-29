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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** {@link MetadataAccessor} for {@link SqlServerDataSource}. */
@Internal
public class SqlServerMetadataAccessor implements MetadataAccessor {

    private final SqlServerSourceConfig sourceConfig;

    public SqlServerMetadataAccessor(SqlServerSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public List<String> listNamespaces() {
        return SqlServerSchemaUtils.listTables(sourceConfig).stream()
                .map(TableId::getNamespace)
                .filter(namespace -> namespace != null && !namespace.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return SqlServerSchemaUtils.listTables(sourceConfig).stream()
                .filter(tableId -> namespace == null || namespace.equals(tableId.getNamespace()))
                .map(TableId::getSchemaName)
                .filter(schema -> schema != null && !schema.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        return SqlServerSchemaUtils.listTables(sourceConfig).stream()
                .filter(
                        tableId ->
                                (namespace == null || namespace.equals(tableId.getNamespace()))
                                        && (schemaName == null
                                                || schemaName.equals(tableId.getSchemaName())))
                .collect(Collectors.toList());
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return SqlServerSchemaUtils.getTableSchema(sourceConfig, tableId);
    }
}
