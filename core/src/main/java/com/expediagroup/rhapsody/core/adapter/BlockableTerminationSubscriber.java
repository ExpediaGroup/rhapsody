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
package com.expediagroup.rhapsody.core.adapter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.expediagroup.rhapsody.util.Throwing;

import reactor.core.publisher.BaseSubscriber;

final class BlockableTerminationSubscriber<T> extends BaseSubscriber<T> {

    private final CompletableFuture<Void> future = new CompletableFuture<>();

    public void block() {
        try {
            future.get();
        } catch (InterruptedException e) {
            throw Throwing.propagate(e, IllegalStateException::new);
        } catch (ExecutionException e) {
            throw Throwing.propagate(e.getCause());
        }
    }

    public void block(Duration timeout) {
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Timeout on blocking read for duration=" + timeout, e);
        } catch (InterruptedException e) {
            throw Throwing.propagate(e, IllegalStateException::new);
        } catch (ExecutionException e) {
            throw Throwing.propagate(e.getCause());
        }
    }

    @Override
    protected void hookOnComplete() {
        future.complete(null);
    }

    @Override
    protected void hookOnError(Throwable throwable) {
        future.completeExceptionally(throwable);
    }
}
