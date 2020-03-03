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

import java.util.Collection;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.util.Throwing;

/**
 * Decorates data items with the notion of "acknowledgeability". An Acknowledgeable data item is
 * not considered fully processed until <i>either</i> its acknowledger or nacknowledger has been
 * executed. Execution of an acknowledger signifies normal processing completion of the correlated
 * data item, while execution of a nacknowledger indicates abnormal, unexpected, and/or exceptional
 * termination of the processing of the correlated data item. Nacknowledgement must always be
 * qualified by a {@link Throwable Throwable} that further elaborates on the cause of processing
 * termination.
 *
 * <p>Implementations of Acknowledgeable should guarantee joint threadsafe idempotency of
 * acknowledgement. In other words, execution of either the acknowledger or nacknowledger must be
 * threadsafe, and once either is executed, further executions of either should result in no-ops.
 * Implementations are responsible for implementing how to propagate enough information with which
 * to eventually execute acknowledgement. Note that implementations may propagate more than just
 * acknowledgeability information.
 *
 * <p>Acknowledgers and Nacknowledgers referenced by Acknowledgeable implementations must be
 * <strong>safe</strong>. They <i>should not throw Exceptions.</i>
 *
 * @param <T> The type of data item decorated with acknowledgeability
 */
public interface Acknowledgeable<T> extends Headed {

    /**
     * Functional convenience method for creating a Predicate of Acknowledgeables based on the type
     * of data items. This is most convenient when using fluent APIs, and acknowledgement of data
     * items that test negative is desired
     *
     * @param predicate Predicate to apply to data items
     * @param negativeConsumer A consumer of Acknowledgeables used when data items test negative
     * @param <T> The type of data item
     * @param <A> The type of Acknowledgeable being filtered
     * @return A predicate of Acknowledgeable
     */
    static <T, A extends Acknowledgeable<T>> Predicate<A>
    filtering(Predicate<? super T> predicate, Consumer<? super Acknowledgeable<T>> negativeConsumer) {
        return wrapMapping((A acknowledgeable) -> acknowledgeable.filter(predicate, negativeConsumer))::apply;
    }

    /**
     * Functional convenience method for creating a mapping from one Acknowledgeable to a
     * Collection of Acknowledgeables based on the mapping of any single data item to zero-to-many
     * resultant items of a given type. The originating acknowledgement should be executed if/when
     * all resultant Acknowledgeables are acknowledged/nacknowledged at some point in the future.
     *
     * @param mapper Function that produces zero-to-many results from any given data item
     * @param emptyMappingConsumer Consumer of Acknowledgeable if mapping is empty
     * @param <T> The type of data item
     * @param <R> The type of results contained in result Collection
     * @param <C> The type of Collection produced by the mapping
     * @return Function that converts a single Acknowledgeable to a Collection of Acknowledgeables
     */
    static <T, R, C extends Collection<R>> Function<Acknowledgeable<T>, Collection<Acknowledgeable<R>>>
    mappingToMany(Function<? super T, ? extends C> mapper, Consumer<? super Acknowledgeable<T>> emptyMappingConsumer) {
        return wrapMapping(acknowledgeable -> acknowledgeable.mapToMany(mapper, emptyMappingConsumer));
    }

    /**
     * Functional convenience method for creating a Mapping of Acknowledgeables of one data item
     * type to Acknowledgeables of another data item type
     *
     * @param mapper A function that transforms data items of type T in to type R
     * @param <T> The type of data items used as input
     * @param <R> The type of data items to be output
     * @return
     */
    static <T, R> Function<Acknowledgeable<T>, Acknowledgeable<R>> mapping(Function<? super T, ? extends R> mapper) {
        return wrapMapping(acknowledgeable -> acknowledgeable.map(mapper));
    }

    /**
     * Functional convenience method for creating a Publisher of Acknowledgeables of one type from
     * an Acknowledgeable of a data item of another type. The resultant Publisher should execute
     * acknowledgement of the originating Acknowledgeable once the following is true:
     * - The resultant Publisher is completed, errored, or canceled
     * - All emitted Acknowledgeables have had acknowledgement executed
     *
     * @param mapper A function to transform data items in to Publishers of resultant items
     * @param <T> The type of input data items
     * @param <R> The type of output data items
     * @param <P> The type of Publisher produced by the mapping
     * @return A Function that transforms Acknowledgables of type T in to Publishers
     */
    static <T, R, P extends Publisher<R>> Function<Acknowledgeable<T>, Publisher<Acknowledgeable<R>>>
    publishing(Function<? super T, ? extends P> mapper) {
        return wrapMapping(acknowledgeable -> acknowledgeable.publish(mapper));
    }

    /**
     * Functional convenience method for creating a reducer of Acknowledgeables from a reducer of
     * data items
     *
     * @param reducer A binary reducer of data items
     * @param <T> The type of data items
     * @return A reducer of Acknowledgeables with data items of type T
     */
    static <T> BinaryOperator<Acknowledgeable<T>> reducing(BinaryOperator<T> reducer) {
        return (acknowledgeable1, acknowledgeable2) -> acknowledgeable1.reduce(reducer, acknowledgeable2);
    }

    /**
     * Functional convenience method for creating a Consumer of Acknowledgeables from a Consumer of
     * data items
     *
     * @param consumer A Consumer of data items
     * @param andThen Consumer of Acknowledgeable to be applied after data item consumption
     * @param <T> The type of data item
     * @return A Consumer of Acknowledgeables of data items of type T
     */
    static <T> Consumer<Acknowledgeable<T>> consuming(Consumer<? super T> consumer, Consumer<? super Acknowledgeable<T>> andThen) {
        return acknowledgeable -> {
            try {
                acknowledgeable.consume(consumer, andThen);
            } catch (Throwable error) {
                acknowledgeable.nacknowledge(error);
            }
        };
    }

    static <T> Consumer<Acknowledgeable<T>> throwingConsuming(Throwing.Consumer<? super T> consumer, Consumer<? super Acknowledgeable<T>> andThen) {
        return acknowledgeable -> {
            try {
                acknowledgeable.throwingConsume(consumer, andThen);
            } catch (Throwable error) {
                acknowledgeable.nacknowledge(error);
            }
        };
    }

    /**
     * Functional convenience method for safely wrapping the transformation of an Acknowledgeable
     * to some result type
     *
     * @param mapper The transformation to apply to an Acknowledgeable
     * @param <A>    The type of Acknowledgeable to operate on
     * @param <R>    The result type of produced from transformation of an Acknowledgeable
     * @return A Function that safely wraps transformation of Acknowledgeables
     */
    static <A extends Acknowledgeable, R> Function<A, R> wrapMapping(Function<? super A, ? extends R> mapper) {
        return acknowledgeable -> {
            try {
                return mapper.apply(acknowledgeable);
            } catch (Throwable error) {
                acknowledgeable.nacknowledge(error);
                throw Throwing.propagate(error);
            }
        };
    }

    /**
     * Convenience method for combining Acknowledgers. Acknowledgers will be run in the same order
     * that they are provided
     */
    static Runnable combineAcknowledgers(Runnable acknowledger1, Runnable acknowledger2) {
        return () -> {
            acknowledger1.run();
            acknowledger2.run();
        };
    }

    /**
     * Convenience method for combining Nacknowledgers. Nacknowledgers will be run in the same
     * order that they are provided
     */
    static Consumer<Throwable> combineNacknowledgers(Consumer<? super Throwable> nacknowledger1, Consumer<? super Throwable> nacknowledger2) {
        return error -> {
            nacknowledger1.accept(error);
            nacknowledger2.accept(error);
        };
    }

    /**
     * Convenience method for executing Acknowledger
     */
    default void acknowledge() {
        getAcknowledger().run();
    }

    /**
     * Convenience method for executing Nacknowledger
     *
     * @param error An Error that elaborates on the reason for abnormal processing termination
     */
    default void nacknowledge(Throwable error) {
        getNacknowledger().accept(error);
    }

    /**
     * Test this Acknowledgeable's data item against a supplied predicate, and execute the supplied
     * Consumer with this Acknowledgeable if the Predicate tests negative (false)
     *
     * @param predicate        The Predicate to test this Acknowledgeable's data item against
     * @param negativeConsumer Consumer to accept this Acknowledgeable if Predicate returns false
     * @return The result of testing this Acknowledgeable's data item against supplied Predicate
     */
    boolean filter(Predicate<? super T> predicate, Consumer<? super Acknowledgeable<T>> negativeConsumer);

    /**
     * Map this Acknowledgeable in to a Collection of Acknowledgeables using the supplied Mapper.
     * The Mapper will be applied to the data item, and the Collection of results will themselves
     * each be wrapped as Acknowledgeable. The originating acknowledgement should not be executed
     * until all resultant items are acknowledged. The supplied Consumer will be passed this
     * Acknowledgeable if the mapping produces an empty result Collection.
     *
     * @param mapper               Function that produces zero-to-many results from the contained data item
     * @param emptyMappingConsumer Consumer to accept this Acknowledgeable if mapper returns empty
     * @param <R>                  The type of elements in the resultant Collection
     * @param <C>                  The type of Collection produced by the mapper
     * @return The mapped collection of results where each item is Acknowledgeable
     */
    <R, C extends Collection<R>> Collection<Acknowledgeable<R>>
    mapToMany(Function<? super T, ? extends C> mapper, Consumer<? super Acknowledgeable<T>> emptyMappingConsumer);

    /**
     * Map this Acknowledgeable's data item to another type, producing an Acknowledgeable of the
     * result type
     *
     * @param mapper Function to be applied to the data item
     * @param <R>    The resultant data item type
     * @return An Acknowledgeable of the resultant type
     */
    <R> Acknowledgeable<R> map(Function<? super T, ? extends R> mapper);

    /**
     * Map this Acknowledgeable to a {@link org.reactivestreams.Publisher Publisher} of results.
     * This Acknowledgeable's acknowledgement should be executed once the following is true:
     * - The resultant Publisher has been terminated or canceled
     * - All emitted results have been acknowledged or nacknowledged
     *
     * @param mapper Transforms this Acknowledgeable's data item in to a Publisher of results
     * @param <R>    The type of results to be published
     * @param <P>    The type of Publisher to emit results
     * @return a Publisher of results
     */
    <R, P extends Publisher<R>> Publisher<Acknowledgeable<R>> publish(Function<? super T, ? extends P> mapper);

    /**
     * Apply a reduction on this Acknowledgeable with another, producing a reduced Acknowledgeable
     *
     * @param reducer The reduction to apply
     * @param other   The other Acknowledgeable to apply reduction with
     * @return a reduced Acknowledgeable
     */
    Acknowledgeable<T> reduce(BinaryOperator<T> reducer, Acknowledgeable<? extends T> other);

    /**
     * Consume this Acknowledgeable's Data item with the provided Consumer, followed by consumption
     * of this Acknowledgeable (i.e. Acknowledgeable::acknowledge) upon not-exceptional completion
     * of the data item consumer
     *
     * @param consumer The Consumer to accept this Acknowledgeable's Data item
     * @param andThen  A Consumer to accept this Acknowledgeable after consumption of data item
     */
    void consume(Consumer<? super T> consumer, Consumer<? super Acknowledgeable<T>> andThen);

    /**
     * Consume this Acknowledgeable's Data item with the provided Throwing Consumer, followed by
     * consumption of this Acknowledgeable (i.e. Acknowledgeable::acknowledge) upon non-exceptional
     * completion of the data item consumer
     *
     * @param consumer The Throwing Consumer to accept this Acknowledgeable's Data item
     * @param andThen  A Consumer to accept this Acknowledgeable after consumption of data item
     */
    void throwingConsume(Throwing.Consumer<? super T> consumer, Consumer<? super Acknowledgeable<T>> andThen) throws Throwable;

    /**
     * Method for allowing propagation of any metadata (like tracing context) from this
     * Acknowledgeable to an Acknowledgeable composed with the supplied Acknowledger and
     * Nacknowledger
     *
     * @param result An Acknowledgeable backed by the supplied Acknowledger and Nacknowledger
     * @param <R>    The type of data item being propagated
     * @return An Acknowledgeable of the resultant type
     */
    <R> Acknowledgeable<R> propagate(R result, Runnable acknowledger, Consumer<? super Throwable> nacknowledger);

    /**
     * Retrieve this Acknowledgeable's data item
     */
    T get();

    /**
     * Retrieve this Acknowledgeable's Acknowledger
     */
    Runnable getAcknowledger();

    /**
     * Retrieve this Acknowledgeable's Nacknowledger
     */
    Consumer<? super Throwable> getNacknowledger();
}
