package store.client.display;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import store.common.Mappable;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Mappable formatting for humans.
 */
final class MappableFormatting {

    private static final String TRANSACTION_ID = "transactionId";
    private static final String LENGTH = "length";
    private static final Collection<String> REMOVALS = Arrays.asList(TRANSACTION_ID);
    private static final Map<ValueType, Formatter> FORMATTERS = new EnumMap<>(ValueType.class);

    static {
        FORMATTERS.put(ValueType.DATE, new Formatter() {
            @Override
            public Value apply(Value value) {
                return Value.of(DateTimeFormat
                        .longDateTime()
                        .withZone(DateTimeZone.getDefault())
                        .print(value.asInstant()));
            }
        });
        FORMATTERS.put(ValueType.HASH, new Formatter() {
            @Override
            public Value apply(Value value) {
                return Value.of(value.asHash().asHexadecimalString());
            }
        });
        FORMATTERS.put(ValueType.OBJECT, new Formatter() {
            @Override
            public Value apply(Value value) {
                Map<String, Value> map = new LinkedHashMap<>();
                for (Entry<String, Value> entry : value.asMap().entrySet()) {
                    map.put(entry.getKey(), formatValue(entry.getValue()));
                }
                return Value.of(map);
            }
        });
        FORMATTERS.put(ValueType.ARRAY, new Formatter() {
            @Override
            public Value apply(Value value) {
                List<Value> list = new ArrayList<>();
                for (Value item : value.asList()) {
                    list.add(formatValue(item));
                }
                return Value.of(list);
            }
        });
    }

    private interface Formatter {

        Value apply(Value value);
    }

    private MappableFormatting() {
    }

    public static Value format(Mappable mappable) {
        Map<String, Value> map = new LinkedHashMap<>();
        for (Entry<String, Value> entry : mappable.toMap().entrySet()) {
            formatAndPut(map, entry.getKey(), entry.getValue());
        }
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
