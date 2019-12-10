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

import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JaxbBodySerDeTest {

    @Test
    public void dataCanBeSerializedToXmlAndBack() {
        JaxbBodySerializer<AbstractXmlData> serializer = new JaxbBodySerializer<>();
        JaxbBodyDeserializer<AbstractXmlData> deserializer = new JaxbBodyDeserializer<>();

        serializer.configure(Collections.singletonMap(JaxbBodySerDe.JAXB_PACKAGES_PROPERTY, AbstractXmlData.class.getPackage().getName()));
        deserializer.configure(Collections.singletonMap(JaxbBodySerDe.JAXB_PACKAGES_PROPERTY, AbstractXmlData.class.getPackage().getName()));

        ConcreteXmlData data = new ConcreteXmlData();
        data.setData("DATA");

        byte[] serialized = serializer.serialize(data);

        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><concreteXmlData><data>DATA</data></concreteXmlData>", new String(serialized));

        AbstractXmlData result = deserializer.deserialize(serialized);

        assertTrue(result instanceof ConcreteXmlData);
        assertEquals(data, result);
    }
}