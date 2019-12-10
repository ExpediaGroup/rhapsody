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
package com.expediagroup.rhapsody.rabbitmq.serde;

import java.io.ByteArrayOutputStream;

public class JaxbBodySerializer<T> extends JaxbBodySerDe implements BodySerializer<T> {

    @Override
    public byte[] serialize(T t) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            jaxbContext.createMarshaller().marshal(t, outputStream);
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to marshal datum in to XML", e);
        }
    }
}
