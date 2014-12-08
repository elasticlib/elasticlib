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