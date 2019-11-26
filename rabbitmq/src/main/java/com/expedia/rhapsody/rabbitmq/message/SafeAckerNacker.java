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

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SafeAckerNacker implements AckerNacker {

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeAckerNacker.class);

    private final Acker acker;

    private final Nacker nacker;

    private final Consumer<? super Throwable> errorEmitter;

    public SafeAckerNacker(Acker acker, Nacker nacker, Consumer<? super Throwable> errorEmitter) {
        this.acker = acker;
        this.nacker = nacker;
        this.errorEmitter = errorEmitter;
    }

    @Override
    public void ack(AckType type) {
        try {
            acker.ack(type);
        } catch (Throwable e) {
            LOGGER.error("Failed to ack", e);
            errorEmitter.accept(e);
        }
    }

    @Override
    public void nack(NackType type) {
        try {
            nacker.nack(type);
        } catch (Throwable e) {
            LOGGER.error("Failed to nack", e);
            errorEmitter.accept(e);
        }
    }
}
