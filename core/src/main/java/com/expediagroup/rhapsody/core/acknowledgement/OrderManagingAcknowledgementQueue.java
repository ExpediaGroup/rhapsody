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
package com.expediagroup.rhapsody.core.acknowledgement;

import java.util.function.Function;

final class OrderManagingAcknowledgementQueue extends AcknowledgementQueue {

    private OrderManagingAcknowledgementQueue(boolean executeErrorsImmediately) {
        super(executeErrorsImmediately);
    }

    public static OrderManagingAcknowledgementQueue newWithImmediateErrors() {
        return new OrderManagingAcknowledgementQueue(true);
    }

    public static OrderManagingAcknowledgementQueue newWithInOrderErrors() {
        return new OrderManagingAcknowledgementQueue(false);
    }

    @Override
    protected boolean complete(InFlight inFlight, Function<InFlight, Boolean> completer) {
        return completer.apply(inFlight);
    }
}
