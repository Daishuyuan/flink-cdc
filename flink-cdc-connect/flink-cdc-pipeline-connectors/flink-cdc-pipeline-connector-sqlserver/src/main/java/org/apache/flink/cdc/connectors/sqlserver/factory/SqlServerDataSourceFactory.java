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

package org.apache.flink.cdc.connectors.sqlserver.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.utils.Predicates;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSource;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.table.SqlServerReadableMetadata;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;
import org.apache.flink.table.api.ValidationException;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.DATABASE_NAME;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.sqlserver.source.SqlServerDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link SqlServerDataSource}. */
@Internal
public class SqlServerDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "sqlserver";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PROPERTIES_PREFIX, DEBEZIUM_OPTIONS_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        String hostname = config.get(HOSTNAME);
        int port = config.get(PORT);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tables = config.get(TABLES);
        String tablesExclude = config.get(TABLES_EXCLUDE);
        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        StartupOptions startupOptions = getStartupOptions(config);
        boolean includeSchemaChanges = config.get(SCHEMA_CHANGE_ENABLED);

        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        boolean closeIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean isAssignUnboundedChunkFirst =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        Map<String, String> configMap = config.toMap();

        SqlServerSourceConfigFactory configFactory =
                new SqlServerSourceConfigFactory()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseName)
                        .tableList(".*")
                        .serverTimeZone(serverTimeZone)
                        .startupOptions(startupOptions)
                        .fetchSize(fetchSize)
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorUpper(distributionFactorUpper)
                        .distributionFactorLower(distributionFactorLower)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .closeIdleReaders(closeIdleReaders)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .assignUnboundedChunkFirst(isAssignUnboundedChunkFirst)
                        .includeSchemaChanges(includeSchemaChanges)
                        .debeziumProperties(getDebeziumProperties(configMap));

        List<TableId> tableIds = SqlServerSchemaUtils.listTables(configFactory.create(0));
        String normalizedTables = normalizeTablePattern(databaseName, tables);
        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(normalizedTables).build();
        List<String> capturedTables = getTableList(tableIds, selectors);
        if (capturedTables.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(tablesExclude)) {
            String normalizedExclude = normalizeTablePattern(databaseName, tablesExclude);
            Selectors selectExclude =
                    new Selectors.SelectorsBuilder().includeTables(normalizedExclude).build();
            List<String> excludeTables = getTableList(tableIds, selectExclude);
            if (!excludeTables.isEmpty()) {
                capturedTables.removeAll(excludeTables);
            }
            if (capturedTables.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cannot find any table by the option 'tables.exclude' = " + tablesExclude);
            }
        }
        configFactory.tableList(capturedTables.toArray(new String[0]));

        String metadataList = config.get(METADATA_LIST);
        List<SqlServerReadableMetadata> readableMetadataList = listReadableMetadata(metadataList);

        return new SqlServerDataSource(configFactory, readableMetadataList);
    }

    private List<SqlServerReadableMetadata> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return new ArrayList<>();
        }
        Set<String> readableMetadataList =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<SqlServerReadableMetadata> foundMetadata = new ArrayList<>();
        for (SqlServerReadableMetadata metadata : SqlServerReadableMetadata.values()) {
            if (readableMetadataList.contains(metadata.getKey())) {
                foundMetadata.add(metadata);
                readableMetadataList.remove(metadata.getKey());
            }
        }
        if (readableMetadataList.isEmpty()) {
            return foundMetadata;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] cannot be found in SQL Server metadata.",
                        String.join(", ", readableMetadataList)));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(TABLES_EXCLUDE);
        options.add(SERVER_TIME_ZONE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SCHEMA_CHANGE_ENABLED);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(METADATA_LIST);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static List<String> getTableList(
            @Nullable List<TableId> tableIdList, Selectors selectors) {
        return tableIdList.stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);
        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    private static String normalizeTablePattern(String databaseName, String tableInclusions) {
        if (StringUtils.isNullOrWhitespaceOnly(tableInclusions)) {
            return tableInclusions;
        }
        Set<String> tableSplitSet =
                Predicates.setOf(
                        tableInclusions,
                        Predicates.RegExSplitterByComma::split,
                        (str) -> str);
        List<String> normalized = new ArrayList<>();
        for (String tableSplit : tableSplitSet) {
            List<String> tableIdList =
                    Predicates.listOf(
                            tableSplit, Predicates.RegExSplitterByDot::split, (str) -> str);
            if (tableIdList.size() == 2 && !StringUtils.isNullOrWhitespaceOnly(databaseName)) {
                normalized.add(
                        String.format(
                                "%s.%s.%s",
                                databaseName,
                                tableIdList.get(0),
                                tableIdList.get(1)));
            } else {
                normalized.add(String.join(".", tableIdList));
            }
        }
        return String.join(",", normalized);
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
