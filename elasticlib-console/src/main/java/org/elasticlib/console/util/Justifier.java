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
package org.elasticlib.console.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Splitter.on;
import static java.lang.Math.min;
import static java.lang.System.lineSeparator;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.Iterator;
import java.util.List;
import static java.util.stream.Collectors.joining;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility class that justifies text.
 */
public class Justifier {

    private static final String SPACE = " ";

    private final int width;
    private final int paddingLeft;
    private final int paddingRight;

    private Justifier(int width, int paddingLeft, int paddingRight) {
        this.width = width;
        this.paddingLeft = paddingLeft;
        this.paddingRight = paddingRight;
    }

    /**
     * Creates a new Justifier instance which outputs justified text at supplied width, without padding.
     *
     * @param value Output text width.
     * @return A new Justifier instance.
     */
    public static Justifier width(int value) {
        checkArgument(value > 0);
        return new Justifier(value, 0, 0);
    }

    /**
     * Provides a copy of this justifier that adds left padding to outputted text.
     *
     * @param value Left padding to insert.
     * @return A new Justifier instance.
     */
    public Justifier paddingLeft(int value) {
        checkArgument(value >= 0);
        return new Justifier(width, value, paddingRight);
    }

    /**
     * Provides a copy of this justifier that adds right padding to outputted text.
     *
     * @param value Right padding to insert.
     * @return A new Justifier instance.
     */
    public Justifier paddingRight(int value) {
        checkArgument(value >= 0);
        return new Justifier(width, paddingLeft, value);
    }

    /**
     * Justifies (and optionally pads) supplied text.
     *
     * @param text Some text.
     * @return Supplied text, justified and padded.
     */
    public String justify(String text) {
        return stream(on(lineSeparator()).split(text))
                .map(this::lines)
                .flatMap(x -> x.stream().map(Line::justify))
                .map(this::addPadding)
                .collect(joining(lineSeparator()));
    }

    private static <T> Stream<T> stream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private List<Line> lines(String paragraph) {
        if (paragraph.isEmpty()) {
            return singletonList(Line.empty());
        }
        int innerWidth = width - paddingLeft - paddingRight;
        List<Line> lines = new ArrayList<>();
        List<String> words = new ArrayList<>();
        Iterator<String> it = on(SPACE).split(paragraph).iterator();
        while (it.hasNext()) {
            String word = it.next();
            if (length(words) + 1 + word.length() > innerWidth) {
                lines.add(new Line(words, innerWidth, false));
                words = new ArrayList<>();
            }
            words.add(word);

            if (!it.hasNext()) {
                lines.add(new Line(words, innerWidth, true));
            }
        }
        return lines;
    }

    private static int length(List<String> words) {
        if (words.isEmpty()) {
            return 0;
        }
        return words.stream().mapToInt(String::length).sum() + words.size() - 1;
    }

    private String addPadding(String line) {
        StringBuilder builder = new StringBuilder();
        pad(builder, paddingLeft);
        builder.append(line);
        pad(builder, paddingRight);

        return builder.toString();
    }

    private static void pad(StringBuilder builder, int length) {
        for (int i = 0; i < length; i++) {
            builder.append(SPACE);
        }
    }

    /**
     * Represents a line in a paragraph.
     */
    private static class Line {

        private static final Line EMPTY = new Line(emptyList(), 0, true);

        private final List<String> words;
        private final int width;
        private final boolean isLast;

        /**
         * Constructor.
         *
         * @param words The words that constitute this line
         * @param width The expected width of this line (after justification).
         * @param isLast If this line is the last of its paragraph.
         */
        public Line(List<String> words, int width, boolean isLast) {
            this.words = new ArrayList<>(words);
            this.width = width;
            this.isLast = isLast;
        }

        /**
         * Convenience factory method to get an empty line (that is, without any word).
         *
         * @return A new Line instance.
         */
        public static Line empty() {
            return EMPTY;
        }

        /**
         * Justifies this line (unless it is the last of its paragraph), at width provided during construction.
         *
         * @return A string representation of this line, with justification.
         */
        public String justify() {
            if (isLast || words.size() <= 1) {
                return String.join(SPACE, words);
            }

            Iterator<Integer> paddings = paddings().iterator();
            StringBuilder builder = new StringBuilder();
            words.forEach(word -> {
                if (builder.length() > 0) {
                    pad(builder, paddings.next());
                    builder.append(SPACE);
                }
                builder.append(word);
            });
            return builder.toString();
        }

        private List<Integer> paddings() {
            int totalPadding = width - length(words);
            int unitPadding = totalPadding / (words.size() - 1);
            int remaining = totalPadding % (words.size() - 1);

            List<Integer> paddings = new ArrayList<>(words.size() - 1);
            for (int i = 0; i < words.size() - 1; i++) {
                int padding = min(totalPadding, unitPadding);
                if (remaining > 0) {
                    padding++;
                    remaining--;
                }
                totalPadding -= padding;
                paddings.add(padding);
            }
            return paddings;
        }
    }
}
