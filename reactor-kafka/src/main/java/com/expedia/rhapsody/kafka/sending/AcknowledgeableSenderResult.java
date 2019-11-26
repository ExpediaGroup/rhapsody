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

import java.util.function.Consumer;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.expedia.rhapsody.api.AbstractAcknowledgeable;
import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.AcknowledgeableFactory;
import com.expedia.rhapsody.api.ComposedAcknowledgeable;

import reactor.kafka.sender.SenderResult;

public class AcknowledgeableSenderResult<T> extends AbstractAcknowledgeable<SenderResult<T>> implements SenderResult<T> {

    private final Acknowledgeable<T> acknowledgeable;

    private final SenderResult senderResult;

    private AcknowledgeableSenderResult(Acknowledgeable<T> acknowledgeable, SenderResult senderResult) {
        this.acknowledgeable = acknowledgeable;
        this.senderResult = senderResult;
    }

    public static <T> AcknowledgeableSenderResult<T> fromSenderResult(SenderResult<Acknowledgeable<T>> senderResult) {
        return new AcknowledgeableSenderResult<>(senderResult.correlationMetadata(), senderResult);
    }

    @Override
    public String toString() {
        return String.format("senderResult=%s acknowledgeable=%s", senderResult, acknowledgeable);
    }

    @Override
    public SenderResult<T> get() {
        return this;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledgeable.getAcknowledger();
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return acknowledgeable.getNacknowledger();
    }

    @Override
    public RecordMetadata recordMetadata() {
        return senderResult.recordMetadata();
    }

    @Override
    public Exception exception() {
        return senderResult.exception();
    }

    @Override
    public T correlationMetadata() {
        return acknowledgeable.get();
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return ComposedAcknowledgeable::new;
    }
}