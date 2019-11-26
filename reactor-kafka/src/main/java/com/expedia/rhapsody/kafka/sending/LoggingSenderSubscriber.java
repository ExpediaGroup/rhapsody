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
package com.expedia.rhapsody.kafka.sending;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.BaseSubscriber;

public class LoggingSenderSubscriber<T> extends BaseSubscriber<T> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(FailureLoggingSenderSubscriber.class);

    @Override
    protected void hookOnError(Throwable error) {
        LOGGER.warn("Kafka Sender threw Error", error);
        super.hookOnError(error);
    }
}
