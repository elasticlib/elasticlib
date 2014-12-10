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

/**
 * Define types of write operations on a repository.
 */
public enum Operation {

    /**
     * A creation, that is adding (or readding) a content and some related info.
     */
    CREATE(0x01),
    /**
     * A pure info update, that is a adding some info without creating or deleting associated content.
     */
    UPDATE(0x02),
    /**
     * A content deletion. Info is updated beside.
     */
    DELETE(0x03);
    private final byte code;

    private Operation(int code) {
        this.code = (byte) code;
    }

    /**
     * Provides operation matching with supplied hexadecimal code. Fails if supplied code is unknown.
     *
     * @param code An operation code.
     * @return Corresponding operation.
     */
    public static Operation fromCode(byte code) {
        for (Operation operation : values()) {
            if (operation.code == code) {
                return operation;
            }
        }
        throw new IllegalArgumentException("0x" + Integer.toHexString(code));
    }

    /**
     * Provides operation matching with supplied string argument. Fails if supplied string is unknown.
     *
     * @param arg An operation as a string, as obtained by a call to toString().
     * @return Corresponding operation.
     */
    public static Operation fromString(String arg) {
        return Operation.valueOf(arg.toUpperCase());
    }

    /**
     * Provides operation hexadecimal code.
     *
     * @return A byte.
     */
    public byte getCode() {
        return code;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
