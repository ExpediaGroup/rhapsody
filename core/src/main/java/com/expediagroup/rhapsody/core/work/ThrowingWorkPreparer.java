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

import com.expediagroup.rhapsody.api.Work;
import com.expediagroup.rhapsody.api.WorkPreparer;

public final class ThrowingWorkPreparer<W extends Work> implements WorkPreparer<W> {

    @Override
    public W prepare(W work) throws Throwable {
        if (!work.isPrepared()) {
            throw new IllegalArgumentException("Cannot prepare unprepared Work: subject={}" + work.workHeader().subject());
        }
        return work;
    }
}
