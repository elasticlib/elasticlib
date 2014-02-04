package store.common.value;

import java.text.DateFormat;
import java.util.Date;

class DateValue extends Value {

    private final long time;

    public DateValue(Date value) {
        time = value.getTime();
    }

    @Override
    public ValueType type() {
        return ValueType.DATE;
    }

    @Override
    public Date asDate() {
        return new Date(time);
    }

    @Override
    public String toString() {
        return DateFormat.getDateTimeInstance().format(new Date(time));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + type().hashCode();
        hash = 23 * hash + (int) (time ^ (time >>> 32));
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
        DateValue other = (DateValue) obj;
        return time == other.time;
    }
}
