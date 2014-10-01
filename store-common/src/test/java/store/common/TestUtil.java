package store.common;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import static org.fest.assertions.api.Assertions.assertThat;
import store.common.exception.NodeException;

/**
 * Test utilities.
 */
public final class TestUtil {

    private TestUtil() {
    }

    /**
     * Read a JSON object from a resource file. File is expected to be located in same "package" as calling class.
     *
     * @param clazz Calling class.
     * @param filename Resource filename.
     * @return A JSON object.
     */
    public static JsonObject readJsonObject(Class<?> clazz, String filename) {
        return (JsonObject) readJson(clazz, filename);
    }

    /**
     * Read a JSON array from a resource file. File is expected to be located in same "package" as calling class.
     *
     * @param clazz Calling class.
     * @param filename Resource filename.
     * @return A JSON array.
     */
    public static JsonArray readJsonArray(Class<?> clazz, String filename) {
        return (JsonArray) readJson(clazz, filename);
    }

    /**
     * Read resource file to a string. File is expected to be located in same "package" as calling class.
     *
     * @param clazz Calling class.
     * @param filename Resource filename.
     * @return A String.
     */
    public static String readString(Class<?> clazz, String filename) {
        try (InputStream inputStream = newInputStream(clazz, filename);
                Reader streamReader = new InputStreamReader(inputStream, Charsets.UTF_8)) {

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[1024];
            int length = streamReader.read(buffer);
            while (length > 0) {
                builder.append(buffer, 0, length);
                length = streamReader.read(buffer);
            }
            return builder.toString();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonStructure readJson(Class<?> clazz, String filename) {
        try (InputStream inputStream = newInputStream(clazz, filename);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                JsonReader jsonReader = Json.createReader(reader)) {

            return jsonReader.read();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream newInputStream(Class<?> clazz, String filename) {
        String resource = clazz.getPackage().getName().replace(".", "/") + "/" + filename;
        return clazz.getClassLoader().getResourceAsStream(resource);
    }

    /**
     * Helper fonction for building bytes array.
     *
     * @param values A bytes sequence.
     * @return A new bytes array.
     */
    public static byte[] array(int... values) {
        byte[] array = new byte[values.length];
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) values[i];
        }
        return array;
    }

    /**
     * Asserts that actual and expected have same size. Then for that each pair of their items, asserts that actual and
     * excepted item are instances of the same class and have same message.
     *
     * @param actual The actual exception list.
     * @param expected The expected exception list.
     */
    public static void assertMatches(List<NodeException> actual, List<NodeException> expected) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertMatches(actual.get(i), expected.get(i));
        }
    }

    private static void assertMatches(NodeException actual, NodeException expected) {
        assertThat(actual).hasSameClassAs(expected);
        assertThat(actual.getMessage()).isEqualTo(expected.getMessage());
    }
}
