package org.elasticlib.common.metadata;

import static org.elasticlib.common.metadata.Properties.Common.FILE_NAME;
import static org.elasticlib.common.metadata.Properties.Text.ENCODING;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;

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
}
