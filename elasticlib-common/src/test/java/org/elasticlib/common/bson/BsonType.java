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
package org.elasticlib.common.bson;

final class BsonType {

    public static final byte NULL = 0x01;
    public static final byte HASH = 0x02;
    public static final byte GUID = 0x03;
    public static final byte BINARY = 0x04;
    public static final byte BOOLEAN = 0x05;
    public static final byte INTEGER = 0x06;
    public static final byte DECIMAL = 0x07;
    public static final byte STRING = 0x08;
    public static final byte DATE = 0x09;
    public static final byte OBJECT = 0x0A;
    public static final byte ARRAY = 0x0B;

    private BsonType() {
    }
}
