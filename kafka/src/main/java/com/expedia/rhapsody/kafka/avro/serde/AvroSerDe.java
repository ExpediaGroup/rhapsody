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
package com.expedia.rhapsody.kafka.avro.serde;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public abstract class AvroSerDe extends AbstractKafkaAvroSerDe {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

    protected static void validateByte(byte expectedByte, byte actualByte) {
        if (expectedByte != actualByte) {
            throw new IllegalArgumentException(String.format("Unexpected Byte. expected=%d actual=%d", expectedByte, actualByte));
        }
    }
}
