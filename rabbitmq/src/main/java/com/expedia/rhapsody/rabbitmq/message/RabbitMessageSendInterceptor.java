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
package com.expedia.rhapsody.rabbitmq.message;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.expedia.rhapsody.util.ConfigLoading;
import com.expedia.rhapsody.util.Instantiation;

public interface RabbitMessageSendInterceptor<T> {

    String CLASSES_CONFIG = "send-interceptor-classes";

    static <T> List<RabbitMessageSendInterceptor<T>> createInterceptors(Map<String, ?> configs) {
        List<RabbitMessageSendInterceptor<T>> interceptors = ConfigLoading
            .loadCollection(configs, CLASSES_CONFIG, Instantiation::<RabbitMessageSendInterceptor<T>>one, Collectors.toList())
            .orElseGet(Collections::emptyList);
        interceptors.forEach(interceptor -> interceptor.configure(configs));
        return interceptors;
    }

    void configure(Map<String, ?> properties);

    RabbitMessage<T> onSend(RabbitMessage<T> rabbitMessage);
}
