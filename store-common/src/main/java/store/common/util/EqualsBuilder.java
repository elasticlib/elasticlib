package store.common.util;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import java.util.Arrays;

/**
 * Support class for implementing equals().
 */
public class EqualsBuilder {

    private boolean isEquals = true;

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(Object lhs, Object rhs) {
        if (!isEquals) {
            return this;
        }
        if (lhs == rhs) {
            return this;
        }
        if (lhs == null || rhs == null) {
            isEquals = false;
            return this;
        }
        if (lhs.getClass().isArray()) {
            appendArrays(lhs, rhs);
        } else {
            isEquals = lhs.equals(rhs);
        }
        return this;
    }

    private void appendArrays(Object lhs, Object rhs) {
        if (lhs.getClass() != rhs.getClass()) {
            isEquals = false;

        } else if (lhs instanceof long[]) {
            isEquals = Arrays.equals((long[]) lhs, (long[]) rhs);

        } else if (lhs instanceof int[]) {
            isEquals = Arrays.equals((int[]) lhs, (int[]) rhs);

        } else if (lhs instanceof short[]) {
            isEquals = Arrays.equals((short[]) lhs, (short[]) rhs);

        } else if (lhs instanceof char[]) {
            isEquals = Arrays.equals((char[]) lhs, (char[]) rhs);

        } else if (lhs instanceof byte[]) {
            isEquals = Arrays.equals((byte[]) lhs, (byte[]) rhs);

        } else if (lhs instanceof double[]) {
            isEquals = Arrays.equals((double[]) lhs, (double[]) rhs);

        } else if (lhs instanceof float[]) {
            isEquals = Arrays.equals((float[]) lhs, (float[]) rhs);

        } else if (lhs instanceof boolean[]) {
            isEquals = Arrays.equals((boolean[]) lhs, (boolean[]) rhs);

        } else {
            append((Object[]) lhs, (Object[]) rhs);
        }
    }

    private void append(Object[] lhs, Object[] rhs) {
        if (lhs.length != rhs.length) {
            isEquals = false;
        }
        for (int i = 0; i < lhs.length && isEquals; ++i) {
            append(lhs[i], rhs[i]);
        }
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(long lhs, long rhs) {
        isEquals = isEquals && lhs == rhs;
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(int lhs, int rhs) {
        isEquals = isEquals && lhs == rhs;
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(short lhs, short rhs) {
        isEquals = isEquals && lhs == rhs;
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(char lhs, char rhs) {
        isEquals = isEquals && lhs == rhs;
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(byte lhs, byte rhs) {
        isEquals = isEquals && lhs == rhs;
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(double lhs, double rhs) {
        isEquals = isEquals && doubleToLongBits(lhs) == doubleToLongBits(rhs);
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(float lhs, float rhs) {
        isEquals = isEquals && floatToIntBits(lhs) == floatToIntBits(rhs);
        return this;
    }

    /**
     * Appends checking of supplied arguments equality.
     *
     * @param lhs Left hand side argument
     * @param rhs Left hand side argument
     * @return this
     */
    public EqualsBuilder append(boolean lhs, boolean rhs) {
        isEquals = isEquals && lhs == rhs;
        return this;
    }

    /**
     * Builds result of all supplied checks.
     *
     * @return true if all supplied fields comparisons have succeed.
     */
    public boolean build() {
        return isEquals;
    }
}
