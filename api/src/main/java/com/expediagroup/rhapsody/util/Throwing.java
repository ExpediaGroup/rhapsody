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
package com.expediagroup.rhapsody.util;

import java.util.function.Function;
import java.util.function.Supplier;

public final class Throwing {

    private Throwing() {

    }

    public static <T> Supplier<T> wrap(ThrowingSupplier<T> throwingSupplier) {
        return () -> {
            try {
                return throwingSupplier.tryGet();
            } catch (Throwable error) {
                throw propagate(error);
            }
        };
    }

    public static <T, R> Function<T, R> wrap(ThrowingFunction<T, R> throwingFunction) {
        return t -> {
            try {
                return throwingFunction.tryApply(t);
            } catch (Throwable error) {
                throw propagate(error);
            }
        };
    }

    public static RuntimeException propagate(Throwable throwable) {
        return propagate(throwable, RuntimeException::new);
    }

    public static RuntimeException propagate(Throwable throwable, Function<? super Throwable, ? extends RuntimeException> runtimeExceptionWrapper) {
        return throwable instanceof RuntimeException ? RuntimeException.class.cast(throwable) : runtimeExceptionWrapper.apply(throwable);
    }

    public interface ThrowingSupplier<T> {

        T tryGet() throws Throwable;
    }

    public interface ThrowingFunction<T, R> {

        R tryApply(T t) throws Throwable;
    }
}
