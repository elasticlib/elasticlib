package store.common;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.Properties.Common.FILE_NAME;
import static store.common.Properties.Text.ENCODING;

/**
 * Unit tests.
 */
public class PropertiesTest {

    /**
     * Test.
     */
    @Test
    public void key() {
        assertThat(FILE_NAME.key()).isEqualTo("fileName");
        assertThat(ENCODING.key()).isEqualTo("encoding");
    }

    /**
     * Test.
     */
    @Test
    public void label() {
        assertThat(FILE_NAME.label()).isEqualTo("File name");
        assertThat(ENCODING.label()).isEqualTo("Encoding");
    }
}
