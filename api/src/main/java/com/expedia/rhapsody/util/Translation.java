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
package com.expedia.rhapsody.util;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public final class Translation<T, R> {

    private final T operand;

    private final R result;

    private Translation(T operand, R result) {
        this.operand = operand;
        this.result = result;
    }

    public static <T, R> Translation<T, R> withResult(T operand, R result) {
        // Note there is no null-check on the result
        return new Translation<>(Objects.requireNonNull(operand, "operand"), result);
    }

    public static <T, R> Translation<T, R> noResult(T operand) {
        return new Translation<>(operand, null);
    }

    public void ifResultPresent(Consumer<? super R> resultConsumer) {
        if (hasResult()) {
            resultConsumer.accept(result);
        }
    }

    public Translation<T, R> filter(Predicate<? super R> predicate) {
        return hasResult() && predicate.test(result) ? this : Translation.noResult(operand);
    }

    public <U> Translation<T, U> map(Function<? super R, ? extends U> mapper) {
        return hasResult() ? Translation.withResult(operand, mapper.apply(result)) : (Translation<T, U>) this;
    }

    public <U> Translation<T, U> flatMap(Function<? super R, Translation<R, U>> mapper) {
        if (hasResult()) {
            Translation<R, U> intermediate = mapper.apply(getResult());
            return intermediate.hasResult() ? Translation.withResult(operand, intermediate.getResult()) : Translation.noResult(operand);
        } else {
            return (Translation<T, U>) this;
        }
    }

    public <U> Translation<T, U> flatMapOptional(Function<? super R, Optional<? extends U>> mapper) {
        if (hasResult()) {
            return mapper.apply(getResult())
                .<Translation<T, U>>map(mapped -> Translation.withResult(operand, mapped))
                .orElseGet(() -> Translation.noResult(operand));
        } else {
            return (Translation<T, U>) this;
        }
    }

    public T getOperand() {
        return operand;
    }

    public boolean hasResult() {
        return result != null;
    }

    public Optional<R> result() {
        return Optional.ofNullable(result);
    }

    public R getResult() {
        return Objects.requireNonNull(result, () -> "Cannot get Result when none is present for operand: " + operand);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Translation<?, ?> that = (Translation<?, ?>) o;
        return Objects.equals(operand, that.operand) &&
            Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand, result);
    }
}
