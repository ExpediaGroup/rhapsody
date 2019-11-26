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
package com.expedia.rhapsody.test;

import com.expedia.rhapsody.api.FailureConsumer;

public final class TestFailureReference<T> implements FailureConsumer<T> {

    private T t;

    private Throwable error;

    @Override
    public void accept(T t, Throwable error) {
        this.t = t;
        this.error = error;
    }

    public void clear() {
        this.t = null;
        this.error = null;
    }

    public T get() {
        return t;
    }

    public Throwable getError() {
        return error;
    }
}
