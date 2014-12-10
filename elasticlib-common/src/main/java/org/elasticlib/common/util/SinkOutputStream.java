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
package org.elasticlib.common.util;

import java.io.OutputStream;

/**
 * An output stream that actually does nothing.
 */
public class SinkOutputStream extends OutputStream {

    private static final SinkOutputStream INSTANCE = new SinkOutputStream();

    private SinkOutputStream() {
    }

    /**
     * @return the singleton instance.
     */
    public static OutputStream sink() {
        return INSTANCE;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        // Actually does nothing.
    }

    @Override
    public void write(int b) {
        // Actually does nothing.
    }
}
