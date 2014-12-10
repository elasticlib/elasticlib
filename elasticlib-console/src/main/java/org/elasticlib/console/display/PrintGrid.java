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

import java.util.ArrayList;
import java.util.List;
import static java.util.stream.Collectors.toList;

/**
 * Represents a text-based two-dimensional printable grid.
 */
class PrintGrid {

    private final List<StringBuilder> builders = new ArrayList<>();

    public PrintGrid(int width, int height) {
        for (int y = 0; y < height; y++) {
            builders.add(new StringBuilder());
            for (int x = 0; x < width; x++) {
                builders.get(y).append(" ");
            }
        }
    }

    public int getHeight() {
        return builders.size();
    }

    public void print(int x, int y, String text) {
        StringBuilder line = builders.get(y);
        for (int i = 0; i < text.length(); i++) {
            line.setCharAt(x + i, text.charAt(i));
        }
    }

    public List<String> render() {
        return builders.stream()
                .map(x -> x.toString().trim())
                .collect(toList());
    }
}
