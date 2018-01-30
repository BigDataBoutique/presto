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

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.util.Locale.ENGLISH;

public final class ElasticQueryRunner extends DistributedQueryRunner {

    private static final String TPCH_SCHEMA = "tpch";

    private final EmbeddedElastic embeddedElastic;

    private ElasticQueryRunner(Session defaultSession, int workersCount) throws Exception {

        super(defaultSession, workersCount);

        this.embeddedElastic = EmbeddedElastic.builder()
                .withElasticVersion("5.5.2")
                .withEsJavaOpts("-Xms128m -Xmx512m")
                .withStartTimeout(60, TimeUnit.SECONDS)
                .withCleanInstallationDirectoryOnStop(true)
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9501)
                .withSetting(PopularProperties.HTTP_PORT, 9502)

                .withSetting(PopularProperties.CLUSTER_NAME, "presto-easticsearch-tests")

                .build()
                .start();
    }

    public static ElasticQueryRunner createElasticQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        ElasticQueryRunner queryRunner = null;
        try {
            queryRunner = new ElasticQueryRunner(createSession(), 2);
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> properties = ImmutableMap.of(
                    "elasticsearch.nodes",  "localhost:" + queryRunner.embeddedElastic.getHttpPort()
            );

            queryRunner.installPlugin(new ElasticsearchPlugin());
            queryRunner.createCatalog("es-tpch", "elasticsearch", properties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("es-tpch")
                .setSchema(TPCH_SCHEMA)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }

    public void shutdown() {
        close();
        embeddedElastic.stop();
    }
}
