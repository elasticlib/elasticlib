package store.common.value;

import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;

class ListValue extends Value {

    private final List<Value> value;

    public ListValue(List<Value> value) {
        this.value = unmodifiableList(new ArrayList<>(value));
    }

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public List<Value> asList() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + type().hashCode();
        hash = 67 * hash + value.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ListValue other = (ListValue) obj;
        return value.equals(other.value);
    }
}
