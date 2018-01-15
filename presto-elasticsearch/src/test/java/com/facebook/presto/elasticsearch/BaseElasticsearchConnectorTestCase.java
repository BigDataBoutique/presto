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

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Collection;
import java.util.Collections;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class BaseElasticsearchConnectorTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(Netty4Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }
}
