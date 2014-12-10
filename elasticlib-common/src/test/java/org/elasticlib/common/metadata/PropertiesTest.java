/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
