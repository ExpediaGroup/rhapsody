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
package com.expediagroup.rhapsody.core.work;

import java.time.Duration;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.api.FailureConsumer;
import com.expediagroup.rhapsody.api.Work;

public abstract class FailedWorkRecycler<W extends Work, R> implements FailureConsumer<W> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailedWorkRecycler.class);

    private final WorkRecycleConfig config;

    private final Consumer<? super R> recycleConsumer;

    public FailedWorkRecycler(WorkRecycleConfig config, Consumer<? super R> recycleConsumer) {
        this.config = config;
        this.recycleConsumer = recycleConsumer;
    }

    @Override
    public void accept(W work, Throwable error) {
        if (isRecyclable(work, error)) {
            R recycled = recycle(work, error);
            hookOnRecycle(work, recycled, error);
            recycleConsumer.accept(recycled);
        } else {
            hookOnDrop(work, error);
        }
    }

    protected boolean isRecyclable(W work, Throwable error) {
        return !isRecycleExpired(work) && !isRecycleMaxedOut(work) && isRecyclableError(error);
    }

    protected void hookOnRecycle(W work, R recycled, Throwable error) {
        LOGGER.warn("Recycling failure of Work={}: subject={} recycled={} error={}",
            work.getClass().getSimpleName(), work.workHeader().subject(), recycled, error.getMessage());
    }

    protected void hookOnDrop(W work, Throwable error) {
        LOGGER.warn("Dropping failed Work={}: subject={} error={}",
            work.getClass().getSimpleName(), work.workHeader().subject(), error.getMessage());
    }

    protected abstract R recycle(W work, Throwable error);

    private boolean isRecycleExpired(Work work) {
        Duration recycleExpiration = config.getRecycleExpiration();
        return !recycleExpiration.equals(Duration.ZERO) && recycleExpiration.compareTo(work.sinceInception()) < 0;
    }

    private boolean isRecycleMaxedOut(Work work) {
        long maxRecycleCount = config.getMaxRecycleCount();
        return maxRecycleCount != Long.MAX_VALUE && maxRecycleCount < work.workHeader().recycleCount();
    }

    private boolean isRecyclableError(Throwable error) {
        return !config.getUnrecyclableErrors().contains(error.getClass());
    }
}
