package store.common.value;

import java.text.DateFormat;
import java.util.Date;
import org.joda.time.Instant;

class DateValue extends Value {

    private final long time;

    public DateValue(Instant value) {
        time = value.getMillis();
    }

    @Override
    public ValueType type() {
        return ValueType.DATE;
    }

    @Override
    public Instant asInstant() {
        return new Instant(time);
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
