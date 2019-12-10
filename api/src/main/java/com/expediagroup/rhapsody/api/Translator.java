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

import java.util.Optional;
import java.util.function.Function;

import com.expediagroup.rhapsody.util.Translation;

/**
 * Translators are an extension of Function that produce {@link com.expediagroup.rhapsody.util.Translation}s.
 * A {@code Translation} is the product of the transformation of that message which may or may not have a result.
 * <p>
 * {@code Translator} use {@code Translation} because they contain the originating message and are useful to downstream operations when
 * accessing the original message may be necessary. For instance, processing the result of a Translation may fail, and the fallback option
 * may be to recycle the original message such that it is retried in the future.
 * </p>
 * <p>
 * {@code Translators} are capable of composing themselves with other translators, hence chaining the effects of each translator.
 * This is particularly useful when you need to build a complex transformation pipeline, but you still want to keep the transformation
 * separated in granular steps.
 * E.g.:
 * </p>
 * <pre>
 *     {@code final Translator<A, B> abTranslator = a -> turnIntoB(a);}
 *     {@code final Translator<B, C> bcTranslator = b -> turnIntoC(b);}
 *     {@code final Translator<A, C> acTranslator = abTranslator.andThenTranslate(bcTranslator);}
 *     {@code // or}
 *     {@code final Translator<X, Y> xyTranslator = x -> turnIntoY(x);}
 *     {@code final Translator<Y, Z> yzTranslator = y -> turnIntoZ(y);}
 *     {@code final Translator<X, Z> xzTranslator = yzTranslator.composeTranslation(xyTranslator);}
 * </pre>
 * @param <T> Type of the input message
 * @param <R> Type of the result of the transformation
 */
@FunctionalInterface
public interface Translator<T, R> extends Function<T, Translation<T, R>> {

    static <T, R> Translator<T, R> fromOptional(Function<? super T, Optional<R>> optionalFunction) {
        return t -> optionalFunction.apply(t)
            .map(result -> Translation.withResult(t, result))
            .orElseGet(() -> Translation.noResult(t));
    }

    /**
     * Composes {@code this Translator} with {@code before Translator}.
     * The effects of {@code before} are applied first, andThenTranslate the effects of {@code this}, just like mathematical functions would compose
     * with the operator •.
     * @param before The {@link Translator} whose effects are applied before the effects of {@code this}
     * @param <U> The input type of {@code before Translator}
     * @return a new {@code Translator}
     */
    default <U> Translator<U, R> composeTranslation(Translator<U, T> before) {
        return u -> before.apply(u).flatMap(this);
    }

    /**
     * Composes {@code this Translator} with {@code after Translator}.
     * The effects of {@code this} are applied first, andThenTranslate the effects of {@code after}.
     * @param after The {@link Translator} whose effects are applied after the effects of {@code this}
     * @param <V> The output type of {@code after Translator} and the composed {@code Translator}
     * @return a new {@code Translator}
     */
    default <V> Translator<T, V> andThenTranslate(Translator<R, V> after) {
      return t -> apply(t).flatMap(after);
    }
}
