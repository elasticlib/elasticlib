/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.console.display;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import static java.util.stream.Collectors.toList;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;

/**
 * Mappable formatting for humans.
 */
final class MappableFormatting {

    private static final String TRANSACTION_ID = "transactionId";
    private static final String LENGTH = "length";
    private static final Collection<String> REMOVALS = Arrays.asList(TRANSACTION_ID);
    private static final Map<ValueType, Formatter> FORMATTERS = new EnumMap<>(ValueType.class);

    static {
        FORMATTERS.put(ValueType.DATE, value -> {
            return Value.of(DateTimeFormatter
                    .ofLocalizedDateTime(FormatStyle.LONG)
                    .withZone(ZoneId.systemDefault())
                    .format(value.asInstant()));
        });
        FORMATTERS.put(ValueType.HASH, value -> Value.of(value.asHash().asHexadecimalString()));
        FORMATTERS.put(ValueType.GUID, value -> Value.of(value.asGuid().asHexadecimalString()));
        FORMATTERS.put(ValueType.OBJECT, value -> {
            Map<String, Value> map = new LinkedHashMap<>();
            value.asMap().entrySet().forEach(entry -> {
                map.put(entry.getKey(), formatValue(entry.getValue()));
            });
            return Value.of(map);
        });
        FORMATTERS.put(ValueType.ARRAY, value -> {
            return Value.of(value.asList()
                    .stream()
                    .map(MappableFormatting::formatValue)
                    .collect(toList()));
        });
    }

    private interface Formatter {

        Value apply(Value value);
    }

    private MappableFormatting() {
    }

    public static Value format(Mappable mappable) {
        Map<String, Value> map = new LinkedHashMap<>();
        mappable.toMap().entrySet().forEach(entry -> {
            formatAndPut(map, entry.getKey(), entry.getValue());
        });
        return Value.of(map);
    }

    private static void formatAndPut(Map<String, Value> map, String key, Value value) {
        if (REMOVALS.contains(key)) {
            return;
        }
        if (key.equals(LENGTH)) {
            map.put(key, Value.of(ByteLengthFormatter.format(value.asLong())));

        } else {
            map.put(key, formatValue(value));
        }
    }

    public static Value formatValue(Value value) {
        if (!FORMATTERS.containsKey(value.type())) {
            return value;
        }
        return FORMATTERS.get(value.type()).apply(value);
    }
}
