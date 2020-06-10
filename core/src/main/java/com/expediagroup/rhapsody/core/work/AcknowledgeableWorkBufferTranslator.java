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
import java.util.Optional;
import java.util.stream.Collectors;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.FailureConsumer;
import com.expediagroup.rhapsody.api.Translator;
import com.expediagroup.rhapsody.api.Work;
import com.expediagroup.rhapsody.api.WorkHeader;
import com.expediagroup.rhapsody.api.WorkPreparer;
import com.expediagroup.rhapsody.api.WorkReducer;
import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.util.Throwing;
import com.expediagroup.rhapsody.util.Translation;

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
        List<Acknowledgeable<W>> nonCanceledBuffer = WorkBuffers.collectNonCanceledAcknowledgeable(buffer, Acknowledgeable::acknowledge);
        try {
            return tryTranslate(nonCanceledBuffer)
                .map(result -> Translation.withResult(buffer, result))
                .orElseGet(() -> Translation.noResult(buffer));
        } catch (Throwable error) {
            handleNonCanceledTranslationError(nonCanceledBuffer, error);
            return Translation.noResult(buffer);
        }
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
