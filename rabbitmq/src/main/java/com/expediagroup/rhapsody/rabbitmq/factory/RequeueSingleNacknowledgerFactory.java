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
package com.expediagroup.rhapsody.rabbitmq.factory;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.rabbitmq.message.AckerNacker;
import com.expediagroup.rhapsody.rabbitmq.message.Nacker;
import com.rabbitmq.client.Delivery;

public class RequeueSingleNacknowledgerFactory implements NacknowledgerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequeueSingleNacknowledgerFactory.class);

    @Override
    public Consumer<Throwable> create(Delivery delivery, AckerNacker ackerNacker, Consumer<? super Throwable> errorEmitter) {
        return error -> {
            LOGGER.warn("Nacking Delivery with single requeue due to Error", error);
            ackerNacker.nack(Nacker.NackType.REQUEUE_SINGLE);
        };
    }
}
