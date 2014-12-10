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
package org.elasticlib.common.hash;

import java.security.SecureRandom;

/**
 * Represents a globally unique identifier.
 */
public class Guid extends AbstractKey implements Comparable<Guid> {

    // Generated GUID length in bytes. Same as a standard UUID length.
    private static final int GUID_LENGTH = 16;
    private static final SecureRandom GENERATOR = new SecureRandom();

    /**
     * Byte array based constructor.
     *
     * @param bytes A byte array.
     */
    public Guid(byte[] bytes) {
        super(bytes);
    }

    /**
     * Hexadecimal string based constructor.
     *
     * @param hexadecimal Hexadecimal encoded bytes. Case unsensitive.
     */
    public Guid(String hexadecimal) {
        super(hexadecimal);
    }

    /**
     * Checks if supplied value is a valid encoded GUID.
     *
     * @param value Some text.
     * @return If supplied text represents a valid GUID.
     */
    public static boolean isValid(String value) {
        return value.length() == GUID_LENGTH * 2 && isBase16(value);
    }

    /**
     * Ramdomly generates a globally unique identifier. Thread-safe.
     *
     * @return A new GUID instance.
     */
    public static synchronized Guid random() {
        // Implementation note : Not sure that GUID are security-sensitive, however we use SecureRandom here,
        // expecting stronger Randomness.
        // The SecureRandom class is likely to be thread-safe, but this is not explicitely stated in its documentation.
        // Therefore this method is synchronized to ensure this. Contention might happen, but this does not matter
        // as this method is not expected to intensively used.

        byte[] randomBytes = new byte[GUID_LENGTH];
        GENERATOR.nextBytes(randomBytes);
        return new Guid(randomBytes);
    }

    @Override
    public int compareTo(Guid that) {
        return compareToImpl(that);
    }
}
