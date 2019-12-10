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
package com.expediagroup.rhapsody.kafka.interceptor;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Configurable;

public final class InterceptorRegistry<T extends Configurable>  {

    private final Map<String, T> interceptorsByClientId = new ConcurrentHashMap<>();

    public void registerIfAbsent(String clientId, T interceptor) {
        interceptorsByClientId.putIfAbsent(clientId, interceptor);
    }

    public void unregister(String clientId) {
        interceptorsByClientId.remove(clientId);
    }

    public Collection<T> getAll() {
        return interceptorsByClientId.values();
    }
}
