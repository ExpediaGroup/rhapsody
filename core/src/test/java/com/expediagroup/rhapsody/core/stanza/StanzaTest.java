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
package com.expediagroup.rhapsody.core.stanza;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.junit.Test;

import com.expediagroup.rhapsody.util.Timing;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class StanzaTest {

    @Test(expected = UnsupportedOperationException.class)
    public void startedStanzaCanNotBeStartedAgain() {
        Stanza<StanzaConfig> stanza = new SimpleStanza(count -> {});

        try {
            stanza.start(() -> "name");
        } catch (Exception e) {
            fail("Should be able to successfully start Stanza on first call");
        }

        stanza.start(() -> "name");
    }

    @Test
    public void stanzaCanBeStopped() {
        List<Long> counts = new CopyOnWriteArrayList<>();

        Stanza<StanzaConfig> stanza = new SimpleStanza(counts::add);
        stanza.start(() -> "name");

        Timing.pause(250);

        assertFalse(counts.isEmpty());

        stanza.stop();

        Timing.pause(250);

        int capturedCount = counts.size();

        Timing.pause(250);

        assertEquals(capturedCount, counts.size());
    }

    private static final class SimpleStanza extends Stanza<StanzaConfig> {

        private final Consumer<? super Long> consumer;

        private SimpleStanza(Consumer<? super Long> consumer) {
            this.consumer = consumer;
        }

        @Override
        protected Disposable startDisposable(StanzaConfig config) {
            return Flux.interval(Duration.ofMillis(100L)).subscribe(consumer);
        }
    }
}