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
package org.elasticlib.common.value;

import java.math.BigDecimal;
import static java.util.Objects.requireNonNull;

final class DecimalValue extends Value {

    private final BigDecimal value;

    public DecimalValue(BigDecimal value) {
        this.value = requireNonNull(value);
    }

    @Override
    public ValueType type() {
        return ValueType.DECIMAL;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value;
    }

    @Override
    public String toString() {
        return value.toPlainString();
    }

    @Override
    Object value() {
        return value;
    }
}
