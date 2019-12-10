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
package com.expediagroup.rhapsody.kafka.test;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.expediagroup.rhapsody.zookeeper.test.TestZookeeperFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class TestKafkaFactory {

    public static final String TEST_KAFKA_PROPERTY_PREFIX = "test.kafka.";

    public static final boolean LOCAL_KAFKA = Boolean.valueOf(System.getProperty(TEST_KAFKA_PROPERTY_PREFIX + "local", "true"));

    private static final String TEST_KAFKA_CONNECT = System.getProperty(TEST_KAFKA_PROPERTY_PREFIX + "connect", "localhost:9092");

    private static KafkaConfig kafkaConfig;

    public Map<String, ?> createKafka() {
        return getKafkaConfig().values();
    }

    public static String extractZookeeperConnect(Map<String, ?> kafkaConfig) {
        return Objects.toString(kafkaConfig.get(KafkaConfig.ZkConnectProp()));
    }

    public static String extractSecureConnect(Map<String, ?> kafkaConfig) {
        return extractSecurityProtocol(kafkaConfig) + "://" + extractConnect(kafkaConfig);
    }

    public static String extractSecurityProtocol(Map<String, ?> kafkaConfig) {
        return Objects.toString(kafkaConfig.get(KafkaConfig.InterBrokerSecurityProtocolProp()));
    }

    public static String extractConnect(Map<String, ?> kafkaConfig) {
        return kafkaConfig.get(KafkaConfig.HostNameProp()) + ":" + kafkaConfig.get(KafkaConfig.PortProp());
    }

    public static String extractConnect(URL connectUrl) {
        return connectUrl.getHost() + ":" + connectUrl.getPort();
    }

    public static URL convertToConnectUrl(String connect) {
        try {
            return new URL("http://" + connect);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create URL for Connect: " + connect, e);
        }
    }

    private static KafkaConfig getKafkaConfig() {
        return kafkaConfig == null ? kafkaConfig = initializeKafka() : kafkaConfig;
    }

    private static KafkaConfig initializeKafka() {
        URL zookeeperConnect = new TestZookeeperFactory().createConnect();
        URL kafkaConnect = convertToConnectUrl(TEST_KAFKA_CONNECT);
        KafkaConfig kafkaConfig = new KafkaConfig(createKafkaBrokerConfig(zookeeperConnect, kafkaConnect), true);
        if (LOCAL_KAFKA) {
            startLocalKafka(kafkaConfig);
        }
        return kafkaConfig;
    }

    private static Map<String, Object> createKafkaBrokerConfig(URL zookeeperConnect, URL kafkaConnect) {
        Map<String, Object> kafkaBrokerConfig = new HashMap<>();
        kafkaBrokerConfig.putIfAbsent(KafkaConfig.ZkConnectProp(), extractConnect(zookeeperConnect));
        kafkaBrokerConfig.putIfAbsent(KafkaConfig.HostNameProp(), kafkaConnect.getHost());
        kafkaBrokerConfig.putIfAbsent(KafkaConfig.PortProp(), kafkaConnect.getPort());
        kafkaBrokerConfig.putIfAbsent(KafkaConfig.NumPartitionsProp(), 10);
        kafkaBrokerConfig.putIfAbsent(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        kafkaBrokerConfig.computeIfAbsent(KafkaConfig.LogDirProp(), key -> createLogDirectory().toString());
        return kafkaBrokerConfig;
    }

    private static Path createLogDirectory() {
        try {
            return Files.createTempDirectory(TestKafkaFactory.class.getSimpleName() + "_" + System.currentTimeMillis());
        } catch (Exception e) {
            throw new IllegalStateException("Could not create temporary log directory", e);
        }
    }

    private static void startLocalKafka(KafkaConfig kafkaConfig) {
        try {
            new KafkaServerStartable(kafkaConfig).startup();
        } catch (Exception e) {
            throw new IllegalStateException("Could not start local Kafka Server", e);
        }
    }
}
