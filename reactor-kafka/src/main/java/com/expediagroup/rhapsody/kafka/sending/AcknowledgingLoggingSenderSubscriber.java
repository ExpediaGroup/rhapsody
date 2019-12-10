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
package com.expediagroup.rhapsody.kafka.sending;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.util.TypeResolution;

import reactor.kafka.sender.SenderResult;

public class AcknowledgingLoggingSenderSubscriber<T> extends LoggingSenderSubscriber<Acknowledgeable<SenderResult<T>>> {

    @Override
    protected void hookOnNext(Acknowledgeable<SenderResult<T>> acknowledgeableSenderResult) {
        if (acknowledgeableSenderResult.get().exception() == null) {
            acknowledgeableSenderResult.acknowledge();
        } else {
            LOGGER.warn("Acknowledgeable Kafka Sender Result has Error: correlationType={} result={}",
                TypeResolution.safelyGetClass(acknowledgeableSenderResult.get().correlationMetadata()), acknowledgeableSenderResult.get());
            acknowledgeableSenderResult.nacknowledge(acknowledgeableSenderResult.get().exception());
        }
    }
}
