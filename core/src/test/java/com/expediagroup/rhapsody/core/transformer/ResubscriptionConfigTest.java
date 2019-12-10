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
package com.expediagroup.rhapsody.core.transformer;

import java.time.Duration;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResubscriptionConfigTest {

    @Test
    public void resubscriptionIsEnabledIfDelayIsNonNegative() {
        assertTrue(new ResubscriptionConfig(Duration.ofSeconds(1L)).isEnabled());
        assertTrue(new ResubscriptionConfig(Duration.ofSeconds(0L)).isEnabled());
        assertFalse(new ResubscriptionConfig(Duration.ofSeconds(-1L)).isEnabled());
        assertFalse(new ResubscriptionConfig(Duration.parse("PT-1S")).isEnabled());
    }
}