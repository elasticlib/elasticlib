package org.elasticlib.common.util;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Units tests.
 */
public class EqualsBuilderTest {

    /**
     * Test.
     */
    @Test
    public void appendSameInstanceTest() {
        Object val = "test";
        EqualsBuilder builder = new EqualsBuilder().append(val, val);
        assertThat(builder.build()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void appendNullTest() {
        assertThat(new EqualsBuilder().append(null, null).build()).isTrue();
        assertThat(new EqualsBuilder().append("test", null).build()).isFalse();
        assertThat(new EqualsBuilder().append(null, "test").build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendLongTest() {
        assertThat(new EqualsBuilder().append(1l, 1l).build()).isTrue();
        assertThat(new EqualsBuilder().append(0l, 1l).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append(1l, 1l).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append(0l, 1l).build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendIntTest() {
        assertThat(new EqualsBuilder().append(1, 1).build()).isTrue();
        assertThat(new EqualsBuilder().append(0, 1).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append(1, 1).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append(0, 1).build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendShortTest() {
        assertThat(new EqualsBuilder().append((short) 1, (short) 1).build()).isTrue();
        assertThat(new EqualsBuilder().append((short) 0, (short) 1).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append((short) 1, (short) 1).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append((short) 0, (short) 1).build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendByteTest() {
        assertThat(new EqualsBuilder().append((byte) 1, (byte) 1).build()).isTrue();
        assertThat(new EqualsBuilder().append((byte) 0, (byte) 1).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append((byte) 1, (byte) 1).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append((byte) 0, (byte) 1).build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendCharTest() {
        assertThat(new EqualsBuilder().append('a', 'a').build()).isTrue();
        assertThat(new EqualsBuilder().append('a', 'b').build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append('a', 'a').build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append('a', 'b').build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendBooleanTest() {
        assertThat(new EqualsBuilder().append(true, true).build()).isTrue();
        assertThat(new EqualsBuilder().append(true, false).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append(true, true).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append(true, false).build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendFloatTest() {
        assertThat(new EqualsBuilder().append(0.1f, 0.1f).build()).isTrue();
        assertThat(new EqualsBuilder().append(Float.NaN, Float.NaN).build()).isTrue();
        assertThat(new EqualsBuilder().append(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY).build()).isTrue();

        assertThat(new EqualsBuilder().append(0.1f, 0.2f).build()).isFalse();
        assertThat(new EqualsBuilder().append(0.1f, Float.NaN).build()).isFalse();
        assertThat(new EqualsBuilder().append(Float.NaN, Float.POSITIVE_INFINITY).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append(0.1f, 0.1f).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append(0.1f, 0.2f).build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendDoubleTest() {
        assertThat(new EqualsBuilder().append(0.1d, 0.1d).build()).isTrue();
        assertThat(new EqualsBuilder().append(Double.NaN, Double.NaN).build()).isTrue();
        assertThat(new EqualsBuilder().append(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY).build()).isTrue();

        assertThat(new EqualsBuilder().append(0.1d, 0.2d).build()).isFalse();
        assertThat(new EqualsBuilder().append(0.1d, Double.NaN).build()).isFalse();
        assertThat(new EqualsBuilder().append(Double.NaN, Double.POSITIVE_INFINITY).build()).isFalse();

        assertThat(new EqualsBuilder().append(true, false).append(0.1d, 0.1d).build()).isFalse();
        assertThat(new EqualsBuilder().append(true, false).append(0.1d, 0.2d).build()).isFalse();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "appendEqualValuesDataProvider")
    public Object[][] appendEqualValuesDataProvider() {
        return new Object[][]{
            {"abc", "abc"},
            {new long[]{1, 2}, new long[]{1, 2}},
            {new int[]{1, 2}, new int[]{1, 2}},
            {new short[]{1, 2}, new short[]{1, 2}},
            {new byte[]{1, 2}, new byte[]{1, 2}},
            {new char[]{'a', 'b'}, new char[]{'a', 'b'}},
            {new boolean[]{true, false}, new boolean[]{true, false}},
            {new float[]{0.1f, 0.2f}, new float[]{0.1f, 0.2f}},
            {new double[]{0.1, 0.2}, new double[]{0.1, 0.2}},
            {new String[]{"ab", "c"}, new String[]{"ab", "c"}}
        };
    }

    /**
     * Test.
     *
     * @param lhs Left hand side argument
     * @param rhs Right hand side argument
     */
    @Test(dataProvider = "appendEqualValuesDataProvider")
    public void appendEqualValuesTest(Object lhs, Object rhs) {
        EqualsBuilder builder = new EqualsBuilder().append(lhs, rhs);
        assertThat(builder.build()).isTrue();
    }

    /**
     * Test.
     *
     * @param lhs Left hand side argument
     * @param rhs Right hand side argument
     */
    @Test(dataProvider = "appendEqualValuesDataProvider")
    public void appendWhenAlreadyNotEqualsTest(Object lhs, Object rhs) {
        EqualsBuilder builder = new EqualsBuilder()
                .append(true, false)
                .append(lhs, rhs);

        assertThat(builder.build()).isFalse();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "appendDifferentValuesDataProvider")
    public Object[][] appendDifferentValuesDataProvider() {
        return new Object[][]{
            {"abc", "def"},
            {new long[]{1, 2}, new long[]{1, 3}},
            {new int[]{1, 2}, new int[]{1, 3}},
            {new short[]{1, 2}, new short[]{1, 3}},
            {new byte[]{1, 2}, new byte[]{1, 3}},
            {new char[]{'a', 'b'}, new char[]{'a', 'c'}},
            {new boolean[]{true, false}, new boolean[]{false, false}},
            {new float[]{0.1f, 0.2f}, new float[]{0.1f, 0.3f}},
            {new double[]{0.1, 0.2}, new double[]{0.1, 0.3}},
            {new String[]{"ab", "c"}, new String[]{"ab", "zzz"}},
            {new String[]{"ab", "c"}, new String[]{"ab"}},
            {new String[]{"ab", "c"}, new int[]{42}}
        };
    }

    /**
     * Test.
     *
     * @param lhs Left hand side argument
     * @param rhs Right hand side argument
     */
    @Test(dataProvider = "appendDifferentValuesDataProvider")
    public void appendDifferentValuesTest(Object lhs, Object rhs) {
        EqualsBuilder builder = new EqualsBuilder().append(lhs, rhs);
        assertThat(builder.build()).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void appendEqualsMultidimensionalArrays() {
        EqualsBuilder builder = new EqualsBuilder().append(appendEqualValuesDataProvider(),
                                                           appendEqualValuesDataProvider());
        assertThat(builder.build()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void appendDifferentsMultidimensionalArrays() {
        EqualsBuilder builder = new EqualsBuilder().append(appendEqualValuesDataProvider(),
                                                           appendDifferentValuesDataProvider());
        assertThat(builder.build()).isFalse();
    }
}
