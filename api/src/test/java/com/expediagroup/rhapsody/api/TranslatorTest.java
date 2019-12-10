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

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import com.expediagroup.rhapsody.util.Translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TranslatorTest {

    @Test
    public void transformsInputValuesAndKeepsTheInput() {
        final UUID input = UUID.randomUUID();
        final Translator<UUID, String> uuidToString = i -> Translation.withResult(i, i.toString());
        final Translation<UUID, String> expectedTranslation = Translation.withResult(input, input.toString());
        final Translation<UUID, String> actualTranslation = uuidToString.apply(input);

        assertEquals(expectedTranslation, actualTranslation);
    }

    @Test
    public void preservesTheInputOnTransformationErrors() {
        final Integer input = 0;
        final Translator<Integer, Integer> intToDouble = i -> {
            try {
                return Translation.withResult(i, 1 / i);
            } catch (Exception e) {
                return Translation.noResult(i);
            }
        };
        final Translation<Integer, Integer> actualTranslation = intToDouble.apply(input);

        assertEquals(input, actualTranslation.getOperand());
        assertFalse(actualTranslation.hasResult());
    }

    @Test
    public void composeWith_CombinesTheEffectsOfThisAndOtherTranslator() {
        final UUID input = UUID.randomUUID();

        final Function<UUID, String> uuidToStringFn = u -> u.toString();
        final Translator<UUID, String> uuidToString = u -> Translation.withResult(u, uuidToStringFn.apply(u));

        final Function<String, List<Character>> stringToListOfCharsFn = s -> s.chars()
            .mapToObj(c -> Character.valueOf((char) c))
            .collect(Collectors.toList());
        final Translator<String, List<Character>> stringToListOfChars = s -> Translation.withResult(s, stringToListOfCharsFn.apply(s));

        final Function<UUID, List<Character>> intToListOfCharsFn = stringToListOfCharsFn.compose(uuidToStringFn);
        final Translator<UUID, List<Character>> intToListOfChars = stringToListOfChars.composeTranslation(uuidToString);

        final Translation<UUID, List<Character>> translation = intToListOfChars.apply(input);

        assertEquals(input, translation.getOperand());
        assertEquals(intToListOfCharsFn.apply(input), translation.getResult());
    }

    @Test
    public void then_CombinesTheEffectsOfOtherTranslatorAndThis() {
        final UUID input = UUID.randomUUID();

        final Function<UUID, Long> uuidToLongFn = u -> u.getMostSignificantBits();
        final Translator<UUID, Long> uuidToLong = u -> Translation.withResult(u, uuidToLongFn.apply(u));

        final Function<Long, Double> longToDoubleFn = l -> l / 2.0;
        final Translator<Long, Double> longToDouble = s -> Translation.withResult(s, longToDoubleFn.apply(s));

        final Function<UUID, Double> uuidToDoubleFn = uuidToLongFn.andThen(longToDoubleFn);
        final Translator<UUID, Double> uuidToDouble = uuidToLong.andThenTranslate(longToDouble);

        final Translation<UUID, Double> translation = uuidToDouble.apply(input);

        assertEquals(input, translation.getOperand());
        assertEquals(uuidToDoubleFn.apply(input), translation.getResult());
    }

    @Test
    public void composedTranslationsHandleNoResultMappings() {
        UUID randomUUID = UUID.randomUUID();
        Translator<UUID, String> uuidToString = uuid -> Translation.withResult(uuid, uuid.toString());

        Translator<UUID, Object> uuidToNull = uuidToString.andThenTranslate(Translation::<String, Object>noResult);

        Translation<UUID, Object> andThenTranslation = uuidToNull.apply(randomUUID);

        assertEquals(randomUUID, andThenTranslation.getOperand());
        assertFalse(andThenTranslation.hasResult());

        Translator<Object, UUID> nonTranslator = Translation::noResult;
        Translator<Object, String> objectToString = uuidToString.composeTranslation(nonTranslator);

        Translation<Object, String> composedTranslation = objectToString.apply(randomUUID);

        assertEquals(randomUUID, composedTranslation.getOperand());
        assertFalse(composedTranslation.hasResult());
    }
}
