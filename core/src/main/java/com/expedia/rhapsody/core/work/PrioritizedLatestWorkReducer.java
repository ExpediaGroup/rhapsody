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

import com.expedia.rhapsody.api.Work;
import com.expedia.rhapsody.api.WorkReducer;
import com.expedia.rhapsody.api.WorkType;

public class PrioritizedLatestWorkReducer<W extends Work> implements WorkReducer<W> {

    @Override
    public W reduceTry(W work1, W work2) {
        return chooseLatestByType(work1, work2);
    }

    @Override
    public W reduceFail(W work1, W work2) {
        return chooseLatestByType(work1, work2);
    }

    protected static <W extends Work> W chooseLatestByType(W work1, W work2) {
        int comparedRelevance = WorkType.compareRelevance(work1.workHeader().type(), work2.workHeader().type());
        if (comparedRelevance == 0) {
            return LatestWorkReducer.chooseLatest(work1, work2);
        } else {
            return comparedRelevance > 0 ? work1 : work2;
        }
    }
}
