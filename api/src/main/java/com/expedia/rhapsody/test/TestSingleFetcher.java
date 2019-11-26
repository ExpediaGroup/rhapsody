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

import java.util.Objects;
import java.util.Optional;

import com.expedia.rhapsody.api.Fetcher;

public final class TestSingleFetcher implements Fetcher {

    private final String subject;

    private final Object operand;

    public TestSingleFetcher(String subject, Object operand) {
        this.subject = subject;
        this.operand = operand;
    }

    @Override
    public <E> Optional<E> fetch(String subject) throws Throwable {
        return Objects.equals(this.subject, subject) ? Optional.ofNullable((E) operand) : Optional.empty();
    }
}
