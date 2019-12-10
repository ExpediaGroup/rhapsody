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
package com.expediagroup.rhapsody.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.test.TestAcknowledgeable;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AcknowledgeableTest {

    @Test
    public void headerIsExtractedFromAcknowledgeableValue() {
        Header header = Header.fromMap(Collections.singletonMap("HEY", "MS_PARKER"));

        Acknowledgeable<Headed> acknowledgeable = new ComposedAcknowledgeable<>(() -> header, () -> {}, error -> {});

        assertEquals(Collections.singletonMap("HEY", "MS_PARKER"), acknowledgeable.header().toMap());
    }

    @Test
    public void acknowledgeablesCanBeFiltered() {
        Predicate<Acknowledgeable<String>> predicate = Acknowledgeable.filtering(String::isEmpty, Acknowledgeable::acknowledge);

        TestAcknowledgeable empty = new TestAcknowledgeable("");
        assertTrue(predicate.test(empty));
        assertFalse(empty.isAcknowledged());

        TestAcknowledgeable nonEmpty = new TestAcknowledgeable("DATA");
        assertFalse(predicate.test(nonEmpty));
        assertTrue(nonEmpty.isAcknowledged());
    }

    @Test
    public void exceptionalFilteringNacknowledges() {
        Predicate<Acknowledgeable<String>> predicate = Acknowledgeable.filtering(string -> {
            throw new IllegalArgumentException();
        }, Acknowledgeable::acknowledge);

        TestAcknowledgeable empty = new TestAcknowledgeable("");

        try {
            predicate.test(empty);
        } catch (Exception e) {
            assertFalse(empty.isAcknowledged());
            assertTrue(empty.isNacknowledged());
            return;
        }

        fail();
    }

    @Test
    public void acknowledgersArePropagated() {
        Function<Acknowledgeable<String>, Acknowledgeable<String>> mapping = Acknowledgeable.mapping(String::toLowerCase);
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Acknowledgeable<String> result = mapping.apply(acknowledgeable);

        assertEquals("data", result.get());
        assertFalse(acknowledgeable.isAcknowledged());

        result.acknowledge();
        assertTrue(acknowledgeable.isAcknowledged());
    }

    @Test
    public void emptyManyMappingHasConsumerExecuted() {
        Function<Acknowledgeable<String>, Collection<Acknowledgeable<String>>> mappingToMany =
            Acknowledgeable.mappingToMany(this::extractCharacters, Acknowledgeable::acknowledge);

        TestAcknowledgeable empty = new TestAcknowledgeable("");
        assertTrue(mappingToMany.apply(empty).isEmpty());
        assertTrue(empty.isAcknowledged());
    }

    @Test
    public void acknowledgerIsRunUponManyMappingsBeingAcknowledged() {
        Function<Acknowledgeable<String>, Collection<Acknowledgeable<String>>> mappingToMany =
            Acknowledgeable.mappingToMany(this::extractCharacters, Acknowledgeable::acknowledge);
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        List<Acknowledgeable<String>> result = new ArrayList<>(mappingToMany.apply(acknowledgeable));

        assertEquals(4, result.size());
        assertFalse(acknowledgeable.isAcknowledged());

        result.get(0).acknowledge();
        result.get(1).acknowledge();
        result.get(2).acknowledge();

        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());

        result.get(3).acknowledge();

        assertTrue(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
    }

    @Test
    public void nacknowledgerIsRunWhenAnyMappingsNacknowledge() {
        Function<Acknowledgeable<String>, Collection<Acknowledgeable<String>>> mappingToMany =
            Acknowledgeable.mappingToMany(this::extractCharacters, Acknowledgeable::acknowledge);
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        List<Acknowledgeable<String>> result = new ArrayList<>(mappingToMany.apply(acknowledgeable));

        assertEquals(4, result.size());
        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());

        result.get(0).acknowledge();
        result.get(1).nacknowledge(new IllegalArgumentException());

        assertFalse(acknowledgeable.isAcknowledged());
        assertTrue(acknowledgeable.isNacknowledged());
        assertTrue(acknowledgeable.getError().map(IllegalArgumentException.class::isInstance).orElse(false));

        result.get(2).nacknowledge(new RuntimeException());
        result.get(3).acknowledge();

        assertFalse(acknowledgeable.isAcknowledged());
        assertTrue(acknowledgeable.isNacknowledged());
        assertTrue(acknowledgeable.getError().map(IllegalArgumentException.class::isInstance).orElse(false));
    }

    @Test
    public void nacknowledgerIsNotRunWhenAlreadyAcknowledged() {
        Function<Acknowledgeable<String>, Collection<Acknowledgeable<String>>> mappingToMany =
            Acknowledgeable.mappingToMany(this::extractCharacters, Acknowledgeable::acknowledge);
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        List<Acknowledgeable<String>> result = new ArrayList<>(mappingToMany.apply(acknowledgeable));

        assertEquals(4, result.size());
        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());

        result.get(0).acknowledge();
        result.get(1).acknowledge();
        result.get(2).acknowledge();
        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());

        result.get(2).nacknowledge(new IllegalArgumentException());
        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());

        result.get(3).acknowledge();
        assertTrue(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
    }

    @Test
    public void acknowledgeablesCanBeConsumed() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Acknowledgeable.<String>consuming(System.out::println, Acknowledgeable::acknowledge).accept(acknowledgeable);

        assertTrue(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
    }

    @Test
    public void acknowledgeablesCanBeExceptionallyConsumed() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Acknowledgeable.<String>consuming(string -> {
            throw new IllegalArgumentException();
        }, Acknowledgeable::acknowledge).accept(acknowledgeable);

        assertFalse(acknowledgeable.isAcknowledged());
        assertTrue(acknowledgeable.isNacknowledged());
    }

    @Test
    public void publishedAcknowledgeableAcknowledgesAfterUpstreamCompletionAndAfterDownstreamAcknowledges() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        AtomicReference<Acknowledgeable> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(Acknowledgeable.publishing(stringToChars).apply(acknowledgeable), 3)
            .expectSubscription()
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("D", character), Acknowledgeable::acknowledge))
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("A", character), Acknowledgeable::acknowledge))
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("T", character), Acknowledgeable::acknowledge))
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertFalse(acknowledgeable.isNacknowledged()))
            .thenRequest(1)
            .consumeNextWith(acknowledgeableData -> {
                assertEquals("A", acknowledgeableData.get());
                lastAcknowledgeable.set(acknowledgeableData);
            })
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertFalse(acknowledgeable.isNacknowledged()))
            .expectComplete()
            .verify();

        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
        assertNotNull(lastAcknowledgeable.get());

        lastAcknowledgeable.get().acknowledge();
        assertTrue(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
    }

    @Test
    public void downstreamPublishedAcknowledebalesAreAcknowledgedOnlyOnce() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        StepVerifier.create(Acknowledgeable.publishing(stringToChars).apply(acknowledgeable), 3)
            .expectSubscription()
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("D", character), Acknowledgeable::acknowledge))
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("A", character), Acknowledgeable::acknowledge))
            .consumeNextWith(acknowledgeableData -> {
                assertEquals("T", acknowledgeableData.get());
                acknowledgeableData.acknowledge();
                acknowledgeableData.nacknowledge(new IllegalArgumentException());
            })
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertFalse(acknowledgeable.isNacknowledged()))
            .thenRequest(1L)
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("A", character), Acknowledgeable::acknowledge))
            .then(() -> assertTrue(acknowledgeable.isAcknowledged()))
            .then(() -> assertFalse(acknowledgeable.isNacknowledged()))
            .expectComplete()
            .verify();
    }

    @Test
    public void publishedAcknowledgeableAcknowledgesAfterDownstreamCancelsAndAcknowledges() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))))
            .take(3);

        AtomicReference<Acknowledgeable> lastAcknowledgeable = new AtomicReference<>();
        StepVerifier.create(Acknowledgeable.publishing(stringToChars).apply(acknowledgeable))
            .expectSubscription()
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("D", character), Acknowledgeable::acknowledge))
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("A", character), Acknowledgeable::acknowledge))
            .consumeNextWith(acknowledgeableData -> {
                assertEquals("T", acknowledgeableData.get());
                lastAcknowledgeable.set(acknowledgeableData);
            })
            .expectComplete()
            .verify();

        assertFalse(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
        assertNotNull(lastAcknowledgeable.get());

        lastAcknowledgeable.get().acknowledge();
        assertTrue(acknowledgeable.isAcknowledged());
        assertFalse(acknowledgeable.isNacknowledged());
    }

    @Test
    public void publishedAcknowledgeableNacknowledgesAfterUpstreamError() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        FluxProcessor<String, String> processor = EmitterProcessor.create();
        FluxSink<String> sink = processor.sink();

        Function<String, Flux<String>> stringToFlux = data -> processor;

        StepVerifier.create(Acknowledgeable.publishing(stringToFlux).apply(acknowledgeable))
            .expectSubscription()
            .then(() -> sink.next("D"))
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("D", character), Acknowledgeable::acknowledge))
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertFalse(acknowledgeable.isNacknowledged()))
            .then(() -> sink.error(new IllegalArgumentException()))
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertTrue(acknowledgeable.getError().map(IllegalArgumentException.class::isInstance).orElse(false)))
            .expectError()
            .verify();
    }

    @Test
    public void publishedAcknowledgeableNacknowledgesAfterDownstreamNacknowledgement() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Function<String, Flux<String>> stringToChars = data -> Mono.just(data.chars())
            .flatMapMany(stream -> Flux.fromStream(stream.mapToObj(character -> String.valueOf((char) character))));

        StepVerifier.create(Acknowledgeable.publishing(stringToChars).apply(acknowledgeable), 2)
            .expectSubscription()
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("D", character), Acknowledgeable::acknowledge))
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("A", character), Acknowledgeable::acknowledge))
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertFalse(acknowledgeable.isNacknowledged()))
            .thenRequest(1)
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("T", character),
                acknowledgeableData -> acknowledgeableData.nacknowledge(new IllegalArgumentException())))
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertTrue(acknowledgeable.getError().map(IllegalArgumentException.class::isInstance).orElse(false)))
            .thenRequest(1)
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("A", character), Acknowledgeable::acknowledge))
            .then(() -> assertFalse(acknowledgeable.isAcknowledged()))
            .then(() -> assertTrue(acknowledgeable.getError().map(IllegalArgumentException.class::isInstance).orElse(false)))
            .expectComplete()
            .verify();
    }

    @Test(expected = IllegalStateException.class)
    public void publishedAcknowledgeableCanOnlyBeSubscribedToOnce() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Publisher<Acknowledgeable<String>> publisher = Acknowledgeable.<String, String, Publisher<String>>publishing(Flux::just).apply(acknowledgeable);

        StepVerifier.create(publisher)
            .expectSubscription()
            .consumeNextWith(Acknowledgeable.consuming(character -> assertEquals("DATA", character), Acknowledgeable::acknowledge))
            .expectComplete()
            .verify();

        Flux.from(publisher).subscribe();
    }

    private Collection<String> extractCharacters(String string) {
        return IntStream.range(0, string.length())
            .mapToObj(string::charAt)
            .map(Object::toString)
            .collect(Collectors.toList());
    }
}