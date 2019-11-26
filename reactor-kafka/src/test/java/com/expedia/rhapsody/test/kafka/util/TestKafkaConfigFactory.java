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
package com.expedia.rhapsody.test.kafka.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expedia.rhapsody.kafka.avro.serde.AvroSerDe;
import com.expedia.rhapsody.kafka.avro.serde.ReflectDecoderAvroDeserializer;
import com.expedia.rhapsody.kafka.avro.serde.ReflectEncoderAvroSerializer;
import com.expedia.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expedia.rhapsody.kafka.factory.KafkaSenderFactory;

public final class TestKafkaConfigFactory {

    private TestKafkaConfigFactory() {

    }

    public static KafkaConfigFactory createFactory(String bootstrapServers, String schemaRegistryUrl) {
        KafkaConfigFactory kafkaConfigFactory = new KafkaConfigFactory();
        kafkaConfigFactory.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaConfigFactory.put(CommonClientConfigs.CLIENT_ID_CONFIG, "TEST_CLIENT");
        kafkaConfigFactory.put(ConsumerConfig.GROUP_ID_CONFIG, "TEST_GROUP");
        kafkaConfigFactory.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        kafkaConfigFactory.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConfigFactory.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ReflectEncoderAvroSerializer.class.getName());
        kafkaConfigFactory.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConfigFactory.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ReflectDecoderAvroDeserializer.class.getName());
        kafkaConfigFactory.put(AvroSerDe.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        kafkaConfigFactory.put(KafkaSenderFactory.HEADERS_ENABLED_CONFIG, true);
        return kafkaConfigFactory;
    }
}
