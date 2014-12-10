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
package org.elasticlib.console.display;

/**
 * CSI color codes.
 */
enum Color {

    BOLD_GREEN("32;1"),
    BOLD_BLUE("34;1"),
    RESET("0");
    private final String code;

    private Color(String code) {
        this.code = "\u001b[" + code + "m";
    }

    @Override
    public String toString() {
        return code;
    }
}
