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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * IO utilities.
 */
public final class IoUtil {

    private static final int BUFFER_SIZE = 8192;

    private IoUtil() {
    }

    /**
     * Writes all bytes read from input to each output.
     *
     * @param input Source input-stream.
     * @param outputs Destination output-stream(s).
     * @throws IOException If an IO error happens.
     */
    public static void copy(InputStream input, OutputStream... outputs) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int len = input.read(buffer);
        while (len != -1) {
            for (OutputStream output : outputs) {
                output.write(buffer, 0, len);
            }
            len = input.read(buffer);
        }
    }
}
