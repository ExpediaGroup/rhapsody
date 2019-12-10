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
import com.expediagroup.rhapsody.api.WorkReducer;

public class LatestWorkReducer<W extends Work> implements WorkReducer<W> {

    @Override
    public W reduceTry(W work1, W work2) {
        return chooseLatest(work1, work2);
    }

    @Override
    public W reduceFail(W work1, W work2) {
        return chooseLatest(work1, work2);
    }

    protected static <W extends Work> W chooseLatest(W work1, W work2) {
        return work1.inceptedAfter(work2) ? work1 : work2;
    }
}
