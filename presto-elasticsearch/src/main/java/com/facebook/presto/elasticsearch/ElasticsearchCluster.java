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

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.Closeable;
import java.io.IOException;

public class ElasticsearchCluster implements Closeable {

    private final RestClient restClient;
    private final Sniffer sniffer;

    public ElasticsearchCluster(RestClient restClient, Sniffer sniffer) {
        this.restClient = restClient;
        this.sniffer = sniffer;
    }

    @Override
    public void close() throws IOException {
        sniffer.close();
        restClient.close();
    }

    public RestClient getRestClient() {
        return restClient;
    }
}
