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
package com.expedia.rhapsody.api;

import java.util.function.Consumer;

import org.reactivestreams.Subscription;

final class ComposedSubscription implements Subscription {

    private final Consumer<? super Long> onRequest;

    private final Runnable onCancel;

    public ComposedSubscription(Consumer<? super Long> onRequest, Runnable onCancel) {
        this.onRequest = onRequest;
        this.onCancel = onCancel;
    }

    @Override
    public void request(long n) {
        onRequest.accept(n);
    }

    @Override
    public void cancel() {
        onCancel.run();
    }
}
