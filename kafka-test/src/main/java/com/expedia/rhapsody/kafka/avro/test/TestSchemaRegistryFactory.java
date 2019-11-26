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
package com.expedia.rhapsody.kafka.avro.test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.expedia.rhapsody.kafka.test.TestKafkaFactory;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;

public class TestSchemaRegistryFactory {

    public static final boolean LOCAL_SCHEMA_REGISTRY = Boolean.parseBoolean(System.getProperty("test.schemaregistry.local", "true"));

    private static final String SCHEMAS_CONNECT = System.getProperty("test.schemaregistry.schemas.connect", "localhost:8081");

    private static final String SCHEMAS_TOPIC = System.getProperty("test.schemaregistry.schemas.topic", "_schemas");

    private static URL schemaRegistryConnect;

    public URL createConnect() {
        return schemaRegistryConnect == null ? schemaRegistryConnect = initializeConnect() : schemaRegistryConnect;
    }

    private static URL initializeConnect() {
        return LOCAL_SCHEMA_REGISTRY ? createLocalSchemaRegistry() : TestKafkaFactory.convertToConnectUrl(SCHEMAS_CONNECT);
    }

    private static URL createLocalSchemaRegistry() {
        try {
            URL schemaConnect = TestKafkaFactory.convertToConnectUrl(SCHEMAS_CONNECT);
            Properties registryProperties = new Properties();
            registryProperties.putAll(createLocalSchemaRegistryConfig(schemaConnect, new TestKafkaFactory().createKafka()));
            new SchemaRegistryRestApplication(new SchemaRegistryConfig(registryProperties)).start();
            return schemaConnect;
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Schema App", e);
        }
    }

    private static Map<String, Object> createLocalSchemaRegistryConfig(URL schemaConnect, Map<String, ?> kafkaConfig) {
        Map<String, Object> registryConfig = new HashMap<>();
        registryConfig.put(SchemaRegistryConfig.LISTENERS_CONFIG, schemaConnect.toString());
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, TestKafkaFactory.extractZookeeperConnect(kafkaConfig));
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_SECURITY_PROTOCOL_CONFIG, TestKafkaFactory.extractSecurityProtocol(kafkaConfig));
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractSecureConnect(kafkaConfig));
        registryConfig.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, SCHEMAS_TOPIC);
        return registryConfig;
    }
}
