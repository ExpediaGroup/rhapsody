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
package com.expediagroup.rhapsody.test.kafka.avro;

import java.util.UUID;

public final class TopicDatum<T> {

    private final String topic;

    private final T datum;

    private final boolean isKey;

    public TopicDatum(T data) {
        this(createRandomTopic(), data);
    }

    public TopicDatum(String topic, T datum) {
        this(topic, datum, false);
    }

    public TopicDatum(String topic, T datum, boolean isKey) {
        this.topic = topic;
        this.datum = datum;
        this.isKey = isKey;
    }

    public static String createRandomTopic() {
        return "TEST_TOPIC_" + UUID.randomUUID();
    }

    public String getTopic() {
        return topic;
    }

    public T getDatum() {
        return datum;
    }

    public boolean isKey() {
        return isKey;
    }
}
