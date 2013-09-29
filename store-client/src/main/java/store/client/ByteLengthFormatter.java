package store.client;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;

public final class ByteLengthFormatter {

    private ByteLengthFormatter() {
    }

    public static String format(long length) {
        Unit unit = Unit.of(length);
        String value = format(BigDecimal.valueOf(length).divide(BigDecimal.valueOf(unit.prefix)));

        StringBuilder builder = new StringBuilder();
        builder.append(value)
                .append(" ")
                .append(unit.toString());

        if (unit != Unit.O) {
            builder.append(" (")
                    .append(String.valueOf(length))
                    .append(" ")
                    .append(Unit.O.toString())
                    .append(")");
        }
        return builder.toString();
    }

    private static String format(BigDecimal value) {
        Iterator<String> parts = Splitter.on('.').split(value.toPlainString()).iterator();
        String integerPart = parts.next();
        if (!parts.hasNext()) {
            return integerPart;
        }
        String fractionalPart = parts.next().substring(0, 1);
        return Joiner.on(',').join(integerPart, fractionalPart);
    }

    private static enum Unit {

        O, KO, MO, GO, TO;
        private final long prefix;

        private Unit() {
            this.prefix = BigInteger.valueOf(1000)
                    .pow(ordinal())
                    .longValue();
        }

        public static Unit of(long length) {
            Unit previous = O;
            for (Unit unit : values()) {
                if (unit.prefix > length) {
                    return previous;
                }
                previous = unit;
            }
            return TO;
        }

        @Override
        public String toString() {
            if (this == O) {
                return "octets";
            }
            return name().charAt(0) + name().substring(1).toLowerCase();
        }
    }
}
