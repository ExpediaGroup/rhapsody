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

import java.util.Optional;

import com.expedia.rhapsody.api.Fetcher;
import com.expedia.rhapsody.api.Work;
import com.expedia.rhapsody.api.WorkPreparer;
import com.expedia.rhapsody.util.Throwing;

public abstract class FetchingWorkPreparer<W extends Work, S> implements WorkPreparer<W> {

    protected final Fetcher fetcher;

    protected FetchingWorkPreparer(Fetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public W prepare(W work) throws Throwable {
        return fetchSubjectForWork(work)
            .map(Throwing.wrap(fetchedSubject -> prepareFetchedSubject(work, fetchedSubject)))
            .orElseGet(Throwing.wrap(() -> prepareUnfetchableSubject(work)));
    }

    protected Optional<S> fetchSubjectForWork(W work) throws Throwable {
        return fetcher.fetch(work.workHeader().subject());
    }

    protected abstract W prepareFetchedSubject(W work, S fetchedSubject) throws Throwable;

    protected abstract W prepareUnfetchableSubject(W work) throws Throwable;
}
