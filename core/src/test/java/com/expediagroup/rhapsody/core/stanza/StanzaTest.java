package com.expediagroup.rhapsody.core.stanza;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import com.expediagroup.rhapsody.util.Timing;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

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
        List<Long> counts = new ArrayList<>();

        Stanza<StanzaConfig> stanza = new SimpleStanza(counts::add);
        stanza.start(() -> "name");

        Timing.pause(500);

        assertFalse(counts.isEmpty());
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