package store.common;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;

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

    private static JsonStructure readJson(Class<?> clazz, String filename) {
        String resource = clazz.getPackage().getName().replace(".", "/") + "/" + filename;
        try (InputStream inputStream = clazz.getClassLoader().getResourceAsStream(resource);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                JsonReader jsonReader = Json.createReader(reader)) {

            return jsonReader.read();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
