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

import com.expediagroup.rhapsody.api.Work;

final class WorkRecycling {

    private WorkRecycling() {

    }

    public static boolean isRecyclable(WorkRecycleConfig config, Work work, Throwable error) {
        return !isRecycleExpired(config, work) && !isRecycleMaxedOut(config, work) && isRecyclableError(config, error);
    }

    private static boolean isRecycleExpired(WorkRecycleConfig config, Work work) {
        Duration recycleExpiration = config.getRecycleExpiration();
        return !recycleExpiration.equals(Duration.ZERO) && recycleExpiration.compareTo(work.sinceInception()) < 0;
    }

    private static boolean isRecycleMaxedOut(WorkRecycleConfig config, Work work) {
        long maxRecycleCount = config.getMaxRecycleCount();
        return maxRecycleCount != Long.MAX_VALUE && maxRecycleCount < work.workHeader().recycleCount();
    }

    private static boolean isRecyclableError(WorkRecycleConfig config, Throwable error) {
        return !config.getUnrecyclableErrors().contains(error.getClass());
    }
}
