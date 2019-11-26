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
package com.expedia.rhapsody.kafka.factory;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;

import com.expedia.rhapsody.api.AcknowledgeableFactory;
import com.expedia.rhapsody.util.ConfigLoading;
import com.expedia.rhapsody.util.Instantiation;

public interface AcknowledgeableConsumerRecordFactory<K, V> extends AcknowledgeableFactory<ConsumerRecord<K, V>>, Configurable {

    String PROPERTY = "acknowledgeable.factory";

    static <K, V> AcknowledgeableConsumerRecordFactory<K, V> create(Map<String, ?> properties) {
        AcknowledgeableConsumerRecordFactory<K, V> acknowledgeableConsumerRecordFactory =
            ConfigLoading.load(properties, PROPERTY, Instantiation::<AcknowledgeableConsumerRecordFactory<K, V>>one)
                .orElseGet(DefaultAcknowledgeableConsumerRecordFactory::new);
        acknowledgeableConsumerRecordFactory.configure(properties);
        return acknowledgeableConsumerRecordFactory;
    }
}
