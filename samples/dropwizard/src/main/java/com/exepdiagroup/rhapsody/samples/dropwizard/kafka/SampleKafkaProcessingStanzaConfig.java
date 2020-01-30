/**
 * Copyright 2019 Expedia, Inc.
 *
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
package com.exepdiagroup.rhapsody.samples.dropwizard.kafka;

import java.util.function.Consumer;

import com.expediagroup.rhapsody.core.stanza.StanzaConfig;

public class SampleKafkaProcessingStanzaConfig implements StanzaConfig {

    private final String bootstrapServers;

    private final String topic;

    private final Consumer<String> consumer;

    public SampleKafkaProcessingStanzaConfig(String bootstrapServers, String topic, Consumer<String> consumer) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.consumer = consumer;
    }

    @Override
    public String name() {
        return "sampleKafkaProcessing";
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public Consumer<String> getConsumer() {
        return consumer;
    }
}
