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
package org.elasticlib.common.model;

import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.elasticlib.common.hash.Hash;

/**
 * An output stream that compute a digest from supplied bytes.
 */
public class DigestOutputStream extends OutputStream {

    private static final String ALGORITHM = "SHA";

    private final MessageDigest messageDigest;
    private long totalLength = 0;

    /**
     * Constructor.
     */
    public DigestOutputStream() {
        try {
            messageDigest = MessageDigest.getInstance(ALGORITHM);

        } catch (NoSuchAlgorithmException e) {
            // Actually impossible.
            throw new AssertionError(e);
        }
    }

    @Override
    public void write(int b) {
        messageDigest.update((byte) b);
        totalLength += 1;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        messageDigest.update(bytes, 0, length);
        totalLength += length;
    }

    /**
     * @return The total number of previously written bytes.
     */
    public long getLength() {
        return totalLength;
    }

    /**
     * @return The Hash of previously written bytes.
     */
    public Hash getHash() {
        try {
            MessageDigest clone = (MessageDigest) messageDigest.clone();
            return new Hash(clone.digest());

        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Creates a new digest. Does not affect this stream state, so more data may be added after this operation in order
     * to build other digests.
     *
     * @return A new Digest instance.
     */
    public Digest getDigest() {
        return new Digest(getHash(), getLength());
    }
}
