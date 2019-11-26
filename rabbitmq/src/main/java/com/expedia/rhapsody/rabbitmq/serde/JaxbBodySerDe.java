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
package com.expedia.rhapsody.rabbitmq.serde;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;

import com.expedia.rhapsody.util.ConfigLoading;

public abstract class JaxbBodySerDe implements BodySerDe {

    public static final String JAXB_PACKAGES_PROPERTY = "jaxb-packages";

    protected JAXBContext jaxbContext;

    @Override
    public void configure(Map<String, ?> properties) {
        this.jaxbContext = ConfigLoading.loadCollectionOrThrow(properties, JAXB_PACKAGES_PROPERTY, Function.identity(),
            Collectors.collectingAndThen(Collectors.joining(":"), JaxbBodySerDe::createJaxbContext));
    }

    private static JAXBContext createJaxbContext(String contextPath) {
        try {
            return JAXBContext.newInstance(contextPath);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create JAXB Context", e);
        }
    }
}
