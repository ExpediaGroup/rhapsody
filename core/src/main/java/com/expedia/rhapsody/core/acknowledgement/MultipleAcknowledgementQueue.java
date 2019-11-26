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
package com.expedia.rhapsody.core.acknowledgement;

import java.util.Iterator;
import java.util.function.Function;

final class MultipleAcknowledgementQueue extends AcknowledgementQueue {

    public MultipleAcknowledgementQueue() {
        super(false);
    }

    //TODO This method may possibly be made more performant, but would be non-trivial to do so. At
    // ToW, performance optimization was not absolutely necessary since multiple acknowledgement,
    // particularly bounded multiple acknowledgement, is/was a rare use case; Even so, typical
    // multiple acknowledgement use cases have acknowledgement externally synchronized (i.e. non
    // concurrent), which means there is low probability of Thread contention here, under which
    // condition there is little overhead incurred by the synchronization block. Furthermore,
    // multiple acknowledgement inherently acknowledges at a frequency less-than-or-equal-to the
    // frequency of data emissions (in fact, usually far less than emission frequency), resulting
    // in still less impact by the synchronization block
    @Override
    protected boolean complete(InFlight lastInFlight, Function<InFlight, Boolean> completer) {
        boolean anyCompleted = false;
        synchronized (queue) {
            Iterator<InFlight> iterator = queue.iterator();
            while (lastInFlight.isInProcess() && iterator.hasNext()) {
                anyCompleted |= completer.apply(iterator.next());
            }
        }
        return anyCompleted;
    }
}
