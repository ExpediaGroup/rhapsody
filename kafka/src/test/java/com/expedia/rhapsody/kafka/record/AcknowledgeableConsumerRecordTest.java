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
package com.expedia.rhapsody.kafka.record;

import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import com.expedia.rhapsody.api.Acknowledgeable;

import static org.junit.Assert.assertEquals;

public class AcknowledgeableConsumerRecordTest {

    @Test
    public void headerCanBeExtractedFromConsumerRecord() {
        Headers headers = new RecordHeaders();
        headers.add(RecordHeaderConversion.toHeader("HI", "MOM"));

        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 1, 4, "A", "TEST", headers);

        Acknowledgeable<ConsumerRecord<String, String>> acknowledgeableRecord =
            new DefaultAcknowledgeableConsumerRecord<>(record, () -> {}, error -> {});

        assertEquals(Collections.singletonMap("HI", "MOM"), acknowledgeableRecord.header().toMap());
    }
}