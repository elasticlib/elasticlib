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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * Byte length formatting for humans.
 */
final class ByteLengthFormatter {

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
            this.prefix = BigInteger.valueOf(1024)
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
