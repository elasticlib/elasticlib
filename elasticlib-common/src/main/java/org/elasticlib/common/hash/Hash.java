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

import java.util.Set;
import java.util.TreeSet;
import static org.elasticlib.common.hash.AbstractKey.isBase16;

/**
 * Represents a Hash.
 * <p>
 * Wraps bare bytes and provides additionnal operations.<br>
 * Is comparable in order to sort hashes in ascending lexicographical order over their encoded form.
 */
public final class Hash extends AbstractKey implements Comparable<Hash> {

    // SHA-1 hashes length in bytes.
    private static final int SHA1_LENGTH = 20;

    /**
     * Byte array based constructor.
     *
     * @param bytes A byte array.
     */
    public Hash(byte[] bytes) {
        super(bytes);
    }

    /**
     * Hexadecimal string based constructor.
     *
     * @param hexadecimal Hexadecimal encoded bytes. Case unsensitive.
     */
    public Hash(String hexadecimal) {
        super(hexadecimal);
    }

    /**
     * Checks if supplied value is a valid encoded SHA-1 hash.
     *
     * @param value Some text.
     * @return If supplied text represents a valid SHA-1 hash.
     */
    public static boolean isValid(String value) {
        return value.length() == SHA1_LENGTH * 2 && isBase16(value);
    }

    /**
     * Derives a key from this hash.
     *
     * @param length Key length in characters. Expected to be positive and less than or equal to encoded hash length.
     * @return This hash encoded and truncated to supplied length.
     */
    public String key(int length) {
        return asHexadecimalString().substring(0, length);
    }

    /**
     * Generates the set of all derivable keys for supplied length. The size of produced set is equal to 16 raised to
     * the power of supplied length which quickly becomes excessive. Therefore, This will fail if supplied length if
     * more than or equal to 6.
     *
     *
     * @param keyLength Key length in characters
     * @return A set of keys.
     */
    public static Set<String> keySet(int keyLength) {
        if (keyLength < 0 || keyLength >= 6) {
            throw new IllegalArgumentException();
        }
        int size = size(keyLength);
        Set<String> keySet = new TreeSet<>();
        for (int i = 0; i < size; i++) {
            keySet.add(keyOf(i, keyLength));
        }
        return keySet;
    }

    private static int size(int keyLength) {
        return keyLength == 0 ? 0 : 1 << (4 * keyLength);
    }

    private static String keyOf(int index, int keyLength) {
        String hex = Integer.toHexString(index);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < keyLength - hex.length(); i++) {
            builder.append('0');
        }
        return builder.append(hex).toString();
    }

    @Override
    public int compareTo(Hash that) {
        return compareToImpl(that);
    }
}
