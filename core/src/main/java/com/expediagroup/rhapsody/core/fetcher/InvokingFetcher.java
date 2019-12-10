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
package com.expediagroup.rhapsody.core.fetcher;

import java.lang.reflect.Method;
import java.util.Optional;

import com.expediagroup.rhapsody.api.Fetcher;
import com.expediagroup.rhapsody.util.MethodResolution;

public class InvokingFetcher implements Fetcher {

    private final Object target;

    private final Method fetchMethod;

    public InvokingFetcher(Object target, String fetchMethodName) {
        this(target, MethodResolution.getAccessibleMethod(target.getClass(), fetchMethodName, String.class));
    }

    public InvokingFetcher(Object target, Method fetchMethod) {
        this.target = target;
        this.fetchMethod = fetchMethod;
    }

    @Override
    public <E> Optional<E> fetch(String subject) throws Throwable {
        Object fetched = fetchMethod.invoke(target, subject);
        return fetched instanceof Optional ? Optional.class.cast(fetched) : Optional.ofNullable((E) fetched);
    }
}
