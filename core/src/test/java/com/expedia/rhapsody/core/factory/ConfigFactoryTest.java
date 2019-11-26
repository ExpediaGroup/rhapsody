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
package com.expedia.rhapsody.core.factory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ConfigFactoryTest {

    private static final String CLIENT_ID = "CLIENT_ID";

    private static final String PROPERTY = "PROPERTY";

    private static final String ADDED_PROPERTY = "ADDED_PROPERTY";

    private DummyConfigFactory configFactory;

    @Before
    public void setup() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("client.id", CLIENT_ID);
        properties.put(PROPERTY, "ORIGINAL_VALUE");
        configFactory = new DummyConfigFactory(properties);
    }

    @After
    public void teardown() {
        Set<String> keysToRemove = System.getProperties().keySet().stream()
            .map(Objects::toString)
            .filter(key -> key.startsWith(ConfigFactory.ENVIRONMENTAL_OVERRIDE_PREFIX))
            .collect(Collectors.toSet());
        keysToRemove.forEach(System.getProperties()::remove);
        TestPropertyProvider.resetCount();
    }

    @Test
    public void configFactoryCreatesCorrectly() {
        Map<String, Object> result = configFactory.create(CLIENT_ID);

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertiesCanBeOverridden() {
        System.setProperty(ConfigFactory.ENVIRONMENTAL_OVERRIDE_PREFIX + PROPERTY, "NEW_VALUE");

        Map<String, Object> result = configFactory.create(CLIENT_ID);

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("NEW_VALUE", result.get(PROPERTY));

        System.setProperty(ConfigFactory.ENVIRONMENTAL_OVERRIDE_PREFIX + CLIENT_ID + "." + PROPERTY, "SUPER_NEW_VALUE");

        result = configFactory.create(CLIENT_ID);

        assertEquals(2, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("SUPER_NEW_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertiesCanBeAddedThroughSpecificOverrides() {
        System.setProperty(ConfigFactory.ENVIRONMENTAL_OVERRIDE_PREFIX + CLIENT_ID + "." + ADDED_PROPERTY, "ADDED");

        Map<String, Object> result = configFactory.create(CLIENT_ID);

        assertEquals(3, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals("ADDED", result.get(ADDED_PROPERTY));
    }

    @Test
    public void propertiesCanBeRandomized() {
        configFactory.put("client.id" + ConfigFactory.RANDOMIZE_PROPERTY_SUFFIX, true);

        Map<String, Object> result = configFactory.create(CLIENT_ID);

        assertEquals(2, result.size());
        assertTrue(Objects.toString(result.get("client.id")).startsWith(CLIENT_ID));
        assertNotEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
    }

    @Test
    public void propertyProvidersCanBeConfiguredAsList() {
        configFactory.put(ConfigFactory.PROVIDERS_PROPERTY, Arrays.asList(TestPropertyProvider.class.getName(), TestPropertyProvider.class.getName()));

        Map<String, Object> result = configFactory.create(CLIENT_ID);

        assertEquals(5, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals(result.get(TestPropertyProvider.TEST_KEY + 1), TestPropertyProvider.TEST_VALUE + 1);
        assertEquals(result.get(TestPropertyProvider.TEST_KEY + 2), TestPropertyProvider.TEST_VALUE + 2);
    }

    @Test
    public void propertyProvidersCanBeConfiguredAsCommaSeparatedList() {
        configFactory.put(ConfigFactory.PROVIDERS_PROPERTY, String.format("%s,%s", TestPropertyProvider.class.getName(), TestPropertyProvider.class.getName()));

        Map<String, Object> result = configFactory.create(CLIENT_ID);

        assertEquals(5, result.size());
        assertEquals(CLIENT_ID, result.get("client.id"));
        assertEquals("ORIGINAL_VALUE", result.get(PROPERTY));
        assertEquals(result.get(TestPropertyProvider.TEST_KEY + 1), TestPropertyProvider.TEST_VALUE + 1);
        assertEquals(result.get(TestPropertyProvider.TEST_KEY + 2), TestPropertyProvider.TEST_VALUE + 2);
    }

    private static final class DummyConfigFactory extends ConfigFactory<Map<String, Object>> {

        public DummyConfigFactory(Map<String, Object> properties) {
            properties.forEach(this::put);
        }

        @Override
        protected void validateProperties(Map<String, Object> properties) {

        }

        @Override
        protected Map<String, Object> postProcessProperties(Map<String, Object> properties) {
            return properties;
        }
    }
}