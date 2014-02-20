package store.common.json.schema;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import static com.google.common.io.BaseEncoding.base16;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import static java.util.Arrays.asList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.value.Value;

/**
 * Unit tests.
 */
public class SchemaTest {

    private final Map<String, Value> metadata;
    private final List<Value> listing;
    private final Schema mapSchema;
    private final Schema listSchema;
    private final JsonObject mapSchemaJson;
    private final JsonObject listSchemaJson;

    {
        metadata = new LinkedHashMap<>();
        metadata.put("pi", Value.of(new BigDecimal("3.1415")));
        metadata.put("checksum", Value.of(base16().lowerCase().decode("8d5f3c77e9")));
        metadata.put("text", Value.of("hello"));
        metadata.put("epoch", Value.of(new Date(0)));
        metadata.put("coefficients", Value.of(asList(Value.of(10), Value.of(20), Value.of(30))));
        metadata.put("mapping", Value.of(ImmutableMap.of("yes", Value.of(true),
                                                         "answer", Value.of(42))));
        listing = asList(Value.of(2014L),
                         Value.of("test"),
                         Value.of(false));

        mapSchema = Schema.of("metadata", metadata);
        listSchema = Schema.of("listing", listing);
        mapSchemaJson = read("mapSchema.json");
        listSchemaJson = read("listSchema.json");
    }

    private JsonObject read(String filename) {
        String resource = getClass().getPackage().getName().replace(".", "/") + "/" + filename;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resource);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                JsonReader jsonReader = Json.createReader(reader)) {

            return jsonReader.readObject();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test.
     */
    @Test
    public void titleTest() {
        assertThat(mapSchema.title()).isEqualTo("metadata");
        assertThat(listSchema.title()).isEqualTo("listing");
        assertThat(mapSchema.properties().get("pi").title()).isEmpty();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void listPropertiesTest() {
        listSchema.properties();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void mapItemsTest() {
        mapSchema.items();
    }

    /**
     * Test.
     */
    @Test
    public void equalsTest() {
        assertThat(mapSchema).isEqualTo(mapSchema);
        assertThat(mapSchema).isNotEqualTo(null);
        assertThat(mapSchema).isNotEqualTo(listSchema);
        assertThat(listSchema).isNotEqualTo(mapSchema);
        assertThat(mapSchema).isEqualTo(Schema.of("metadata", metadata));
        assertThat(listSchema).isEqualTo(Schema.of("listing", listing));
        assertThat(mapSchema).isNotEqualTo(Schema.of("", metadata));
    }

    /**
     * Test.
     */
    @Test
    public void hashCodeTest() {
        assertThat(mapSchema.hashCode()).isEqualTo(Schema.of("metadata", metadata).hashCode());
        assertThat(listSchema.hashCode()).isEqualTo(Schema.of("listing", listing).hashCode());
        assertThat(mapSchema.hashCode()).isNotEqualTo(listSchema.hashCode());
    }

    /**
     * Test.
     */
    @Test
    public void writeMapSchemaTest() {
        assertThat(mapSchema.write()).isEqualTo(mapSchemaJson);
    }

    /**
     * Test.
     */
    @Test
    public void writeListSchemaTest() {
        assertThat(listSchema.write()).isEqualTo(listSchemaJson);
    }

    /**
     * Test.
     */
    @Test
    public void readMapSchemaTest() {
        assertThat(Schema.read(mapSchemaJson)).isEqualTo(mapSchema);
    }

    /**
     * Test.
     */
    @Test
    public void readListSchemaTest() {
        assertThat(Schema.read(listSchemaJson)).isEqualTo(listSchema);
    }
}
