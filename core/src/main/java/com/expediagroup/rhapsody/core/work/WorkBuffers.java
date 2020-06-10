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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.Work;
import com.expediagroup.rhapsody.api.WorkHeader;
import com.expediagroup.rhapsody.api.WorkType;

final class WorkBuffers {

    private WorkBuffers() {

    }

    public static <W extends Work> List<W> collectNonCanceled(List<W> buffer) {
        Map<String, WorkType> relevantWorkTypesByMarker = buffer.stream()
            .map(Work::workHeader)
            .collect(Collectors.groupingBy(WorkHeader::marker, WorkType.highestRelevanceReducing(WorkHeader::type)));

        return buffer.stream()
            .filter(work -> isNonCanceled(relevantWorkTypesByMarker, work.workHeader()))
            .collect(Collectors.toList());
    }

    public static <W extends Work> List<Acknowledgeable<W>>
    collectNonCanceledAcknowledgeable(List<Acknowledgeable<W>> buffer, Consumer<? super Acknowledgeable<W>> canceledConsumer) {
        Map<String, WorkType> relevantWorkTypesByMarker = buffer.stream()
            .map(acknowledgeableWork -> acknowledgeableWork.get().workHeader())
            .collect(Collectors.groupingBy(WorkHeader::marker, WorkType.highestRelevanceReducing(WorkHeader::type)));

        return buffer.stream()
            .filter(Acknowledgeable.filtering(work -> isNonCanceled(relevantWorkTypesByMarker, work.workHeader()), canceledConsumer))
            .collect(Collectors.toList());
    }

    private static boolean isNonCanceled(Map<String, WorkType> relevantWorkTypesByMarker, WorkHeader header) {
        return relevantWorkTypesByMarker.get(header.marker()) != WorkType.CANCEL && header.type() != WorkType.CANCEL;
    }
}
