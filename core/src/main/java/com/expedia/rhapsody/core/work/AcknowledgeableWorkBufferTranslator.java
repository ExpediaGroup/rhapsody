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
package com.expedia.rhapsody.core.work;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.FailureConsumer;
import com.expedia.rhapsody.api.Translator;
import com.expedia.rhapsody.api.Work;
import com.expedia.rhapsody.api.WorkHeader;
import com.expedia.rhapsody.api.WorkPreparer;
import com.expedia.rhapsody.api.WorkReducer;
import com.expedia.rhapsody.api.WorkType;
import com.expedia.rhapsody.util.Throwing;
import com.expedia.rhapsody.util.Translation;

public class AcknowledgeableWorkBufferTranslator<W extends Work> implements Translator<List<Acknowledgeable<W>>, Acknowledgeable<W>> {

    private final WorkReducer<W> workReducer;

    private final WorkPreparer<W> workPreparer;

    private final FailureConsumer<W> failureConsumer;

    public AcknowledgeableWorkBufferTranslator(WorkReducer<W> workReducer, WorkPreparer<W> workPreparer, FailureConsumer<W> failureConsumer) {
        this.workReducer = workReducer;
        this.workPreparer = workPreparer;
        this.failureConsumer = failureConsumer;
    }

    @Override
    public Translation<List<Acknowledgeable<W>>, Acknowledgeable<W>> apply(List<Acknowledgeable<W>> buffer) {
        List<Acknowledgeable<W>> nonCanceledBuffer = collectNonCanceled(buffer);
        try {
            return tryTranslate(nonCanceledBuffer)
                .map(result -> Translation.withResult(buffer, result))
                .orElseGet(() -> Translation.noResult(buffer));
        } catch (Throwable error) {
            handleNonCanceledTranslationError(nonCanceledBuffer, error);
            return Translation.noResult(buffer);
        }
    }

    private List<Acknowledgeable<W>> collectNonCanceled(List<Acknowledgeable<W>> buffer) {
        Map<String, WorkType> relevantWorkTypesByMarker = buffer.stream()
            .map(acknowledgeableWork -> acknowledgeableWork.get().workHeader())
            .collect(Collectors.groupingBy(WorkHeader::marker, WorkType.highestRelevanceReducing(WorkHeader::type)));

        return buffer.stream()
            .filter(Acknowledgeable.filtering(work -> isNonCanceled(relevantWorkTypesByMarker, work.workHeader()), Acknowledgeable::acknowledge))
            .collect(Collectors.toList());
    }

    private boolean isNonCanceled(Map<String, WorkType> relevantWorkTypesByMarker, WorkHeader header) {
        return relevantWorkTypesByMarker.get(header.marker()) != WorkType.CANCEL && header.type() != WorkType.CANCEL;
    }

    private Optional<Acknowledgeable<W>> tryTranslate(List<Acknowledgeable<W>> nonCanceledBuffer) {
        return nonCanceledBuffer.stream()
            .reduce(Acknowledgeable.reducing(workReducer::reduceTry))
            .map(reduced -> reduced.map(Throwing.wrap(workPreparer::prepareIfNecessary)));
    }

    private void handleNonCanceledTranslationError(List<Acknowledgeable<W>> nonCanceledBuffer, Throwable error) {
        try {
            nonCanceledBuffer.stream()
                .map(Acknowledgeable::get)
                .reduce(workReducer::reduceFail)
                .ifPresent(fail -> failureConsumer.accept(fail, error));

            nonCanceledBuffer.forEach(Acknowledgeable::acknowledge);
        } catch (Throwable fatalError) {
            nonCanceledBuffer.forEach(acknowledgeable -> acknowledgeable.nacknowledge(fatalError));
        }
    }
}
