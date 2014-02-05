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
    Object value() {
        return time;
    }
}
