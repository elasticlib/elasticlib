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
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.DigestBuilder;

/**
 * IO utilities.
 */
public final class IoUtil {

    private static final int BUFFER_SIZE = 8192;

    private IoUtil() {
    }

    /**
     * Writes all bytes read from input-stream to output-stream.
     *
     * @param input Source input-stream.
     * @param output Destination output-stream.
     * @throws IOException If an IO error happens.
     */
    public static void copy(InputStream input, OutputStream output) throws IOException {
        copyAndDigest(input, output, null);
    }

    /**
     * Writes all bytes read from input-stream to output-stream, and provides a digest of these bytes.
     *
     * @param input Source input-stream.
     * @param output Destination output-stream.
     * @return A digest of copied bytes.
     * @throws IOException If an IO error happens.
     */
    public static Digest copyAndDigest(InputStream input, OutputStream output) throws IOException {
        DigestBuilder builder = new DigestBuilder();
        copyAndDigest(input, output, builder);
        return builder.build();
    }

    /**
     * Writes all bytes read from input-stream to supplied output-stream and digest.
     *
     * @param input Source input-stream.
     * @param output Destination output-stream.
     * @param digest Mutable digest to write to.
     * @throws IOException If an IO error happens.
     */
    public static void copyAndDigest(InputStream input, OutputStream output, DigestBuilder digest) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int len = input.read(buffer);
        while (len != -1) {
            if (digest != null) {
                digest.add(buffer, len);
            }
            output.write(buffer, 0, len);
            len = input.read(buffer);
        }
    }
}
