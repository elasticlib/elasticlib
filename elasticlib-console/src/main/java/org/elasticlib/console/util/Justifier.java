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
import com.google.common.base.Strings;
import static java.lang.Math.min;
import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final List<String> fixed;

    private Justifier(int width, int paddingLeft, int paddingRight, List<String> fixed) {
        this.width = width;
        this.paddingLeft = paddingLeft;
        this.paddingRight = paddingRight;
        this.fixed = fixed;
    }

    /**
     * Creates a new Justifier instance which outputs justified text at supplied width, without padding.
     *
     * @param value Output text width.
     * @return A new Justifier instance.
     */
    public static Justifier width(int value) {
        checkArgument(value > 0);
        return new Justifier(value, 0, 0, emptyList());
    }

    /**
     * Provides a copy of this justifier that adds left padding to outputted text.
     *
     * @param value Left padding to insert.
     * @return A new Justifier instance.
     */
    public Justifier paddingLeft(int value) {
        checkArgument(value >= 0);
        return new Justifier(width, value, paddingRight, fixed);
    }

    /**
     * Provides a copy of this justifier that adds right padding to outputted text.
     *
     * @param value Right padding to insert.
     * @return A new Justifier instance.
     */
    public Justifier paddingRight(int value) {
        checkArgument(value >= 0);
        return new Justifier(width, paddingLeft, value, fixed);
    }

    /**
     * Provides a copy of this justifier that does not insert justification spaces around supplied separator(s).
     *
     * @param separators Separator(s) around which additional spaces should never been added.
     * @return A new Justifier instance.
     */
    public Justifier fixed(String... separators) {
        checkArgument(Arrays.stream(separators).noneMatch(x -> Strings.isNullOrEmpty(x) || x.equals(SPACE)));
        List<String> collector = new ArrayList<>();
        collector.addAll(fixed);
        Arrays.stream(separators).forEach(collector::add);

        return new Justifier(width, paddingLeft, paddingRight, collector);
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
        Iterator<String> it = words(paragraph).iterator();
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

    private Iterable<String> words(String paragraph) {
        return () -> new WordsIterator(paragraph, fixed);
    }

    /**
     * An iterator that provides words in a paragraph.
     */
    private static class WordsIterator implements Iterator<String> {

        private final Iterator<String> source;
        private final List<String> fixed;
        private String buffer;

        public WordsIterator(String paragraph, List<String> fixed) {
            source = on(SPACE).split(paragraph).iterator();
            this.fixed = fixed;
        }

        @Override
        public boolean hasNext() {
            return buffer != null || source.hasNext();
        }

        @Override
        public String next() {
            String current = take();
            if (!hasNext()) {
                return current;
            }
            String next = take();
            if (!fixed.contains(current) && !fixed.contains(next)) {
                buffer = next;
                return current;
            }
            if (fixed.contains(current) || !hasNext()) {
                return join(SPACE, current, next);
            }
            return join(SPACE, current, next, take());
        }

        private String take() {
            if (buffer == null) {
                return source.next();
            }
            String item = buffer;
            buffer = null;
            return item;
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
