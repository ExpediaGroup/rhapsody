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
package com.expedia.rhapsody.rabbitmq.message;

@FunctionalInterface
public interface Nacker {

    enum NackType {
        DISCARD_SINGLE(false, false),
        DISCARD_MULTIPLE(false, true),
        REQUEUE_SINGLE(true, false),
        REQUEUE_MULTIPLE(true, true);

        private final boolean requeue;

        private final boolean multiple;

        NackType(boolean requeue, boolean multiple) {
            this.multiple = multiple;
            this.requeue = requeue;
        }

        public boolean isRequeue() {
            return requeue;
        }

        public boolean isMultiple() {
            return multiple;
        }
    }

    void nack(NackType type);
}
