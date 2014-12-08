package org.elasticlib.common.value;

import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;

final class ListValue extends Value {

    private final List<Value> value;

    public ListValue(List<Value> value) {
        this.value = unmodifiableList(new ArrayList<>(value));
    }

    @Override
    public ValueType type() {
        return ValueType.ARRAY;
    }

    @Override
    public List<Value> asList() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
