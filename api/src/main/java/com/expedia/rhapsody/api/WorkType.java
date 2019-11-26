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

import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Indicates the executional Type of Work to be done.
 * INTENT - Notional work that can be tried at some point in the future.
 * COMMIT - Work that should be tried immediately.
 * RETRY - Work that has been tried and failed. Should try again at some point in the future.
 * CANCEL - Work that has been explicitly canceled AFTER at least intending to execute it.
 */
public enum WorkType {
    INTENT,
    COMMIT,
    RETRY,
    CANCEL;

    public static <T> Collector<T, ?, WorkType> highestRelevanceReducing(Function<? super T, WorkType> typeExtractor) {
        return Collectors.reducing(WorkType.INTENT, typeExtractor, WorkType::chooseHighestRelevance);
    }

    public static WorkType chooseHighestRelevance(WorkType workType1, WorkType workType2) {
        return compareRelevance(workType1, workType2) >= 0 ? workType1 : workType2;
    }

    /**
     * Comparing the "relevance" of two WorkTypes is based on how "obvious" the decision should be
     * about what to do with two related Works of the corresponding Types. Committed Work should
     * always be immediately tried, giving WorkType.COMMIT the highest relevance. In the absence
     * of committed Work, canceled Work then takes precedence as it means the Work can be dropped.
     * Finally, retries are prioritized over intended Work due to the implication of propagated
     * retry context, i.e. Reason for Retry/Failure, Number of Failures, etc.
     */
    public static int compareRelevance(WorkType workType1, WorkType workType2) {
        if (workType1 == workType2) {
            return 0;
        } else if (workType1 == WorkType.COMMIT || workType2 == WorkType.COMMIT) {
            return workType1 == WorkType.COMMIT ? 1 : -1;
        } else if (workType1 == WorkType.CANCEL || workType2 == WorkType.CANCEL) {
            return workType1 == WorkType.CANCEL ? 1 : -1;
        } else {
            return workType1 == WorkType.RETRY ? 1 : -1;
        }
    }
}
