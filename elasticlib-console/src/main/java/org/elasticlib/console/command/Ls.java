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
package org.elasticlib.console.command;

import java.io.IOException;
import static java.lang.Math.max;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.readSymbolicLink;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Comparator.comparing;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.ByteLengthFormatter;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.Directories.workingDirectory;

class Ls extends AbstractCommand {

    Ls() {
        super(Category.MISC);
    }

    @Override
    public String description() {
        return "Print current working directory content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        pad(rows())
                .stream()
                .sorted(comparing(r -> {
                    String fileName = r.get(r.size() - 1);
                    if (fileName.startsWith(".")) {
                        fileName = fileName.substring(1);
                    }
                    return fileName.toLowerCase();
                }))
                .forEach(r -> display.println(String.join(" ", r)));
    }

    private static List<List<Cell>> rows() {
        List<List<Cell>> rows = new ArrayList<>();
        try (DirectoryStream<Path> stream = newDirectoryStream(workingDirectory())) {
            for (Path path : stream) {
                rows.add(row(path));
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
        return rows;
    }

    private static List<Cell> row(Path path) throws IOException {
        List<Cell> values = new ArrayList<>();
        BasicFileAttributes basicAttributes = Files.readAttributes(path,
                                                                   BasicFileAttributes.class,
                                                                   LinkOption.NOFOLLOW_LINKS);

        Optional<PosixFileAttributes> posixFileAttributesOpt = posixFileAttributes(path);
        if (posixFileAttributesOpt.isPresent()) {
            PosixFileAttributes posixAttributes = posixFileAttributesOpt.get();

            values.add(Cell.rightPadded((fileType(basicAttributes) + permissions(posixAttributes))));
            values.add(Cell.rightPadded(posixAttributes.owner().getName()));
            values.add(Cell.rightPadded(posixAttributes.group().getName()));
        }
        values.add(Cell.leftPadded(ByteLengthFormatter.formatShort(basicAttributes.size())));
        values.add(Cell.rightPadded(format(basicAttributes.lastModifiedTime(), "MMM")));
        values.add(Cell.leftPadded(format(basicAttributes.lastModifiedTime(), "e HH:mm")));
        values.add(Cell.rightPadded(path.getFileName().toString() + fileNameComplement(path, basicAttributes)));

        return values;
    }

    private static Optional<PosixFileAttributes> posixFileAttributes(Path path) throws IOException {
        try {
            return Optional.of(Files.readAttributes(path,
                                                    PosixFileAttributes.class,
                                                    LinkOption.NOFOLLOW_LINKS));

        } catch (UnsupportedOperationException e) {
            // Properly checking if posix attributes are supported fails if path corresponds to a broken link.
            return Optional.empty();
        }
    }

    private static String fileType(BasicFileAttributes attributes) {
        if (attributes.isRegularFile()) {
            return "-";
        }
        if (attributes.isDirectory()) {
            return "d";
        }
        if (attributes.isSymbolicLink()) {
            return "l";
        }
        return "o";
    }

    private static String permissions(PosixFileAttributes attributes) {
        Collection<PosixFilePermission> permissions = attributes.permissions();
        return new StringBuilder()
                .append(permissions.contains(OWNER_READ) ? "r" : "-")
                .append(permissions.contains(OWNER_WRITE) ? "w" : "-")
                .append(permissions.contains(OWNER_EXECUTE) ? "x" : "-")
                .append(permissions.contains(GROUP_READ) ? "r" : "-")
                .append(permissions.contains(GROUP_WRITE) ? "w" : "-")
                .append(permissions.contains(GROUP_EXECUTE) ? "x" : "-")
                .append(permissions.contains(OTHERS_READ) ? "r" : "-")
                .append(permissions.contains(OTHERS_WRITE) ? "w" : "-")
                .append(permissions.contains(OTHERS_EXECUTE) ? "x" : "-")
                .toString();
    }

    private static String format(FileTime fileTime, String pattern) {
        return DateTimeFormatter
                .ofPattern(pattern, Locale.getDefault())
                .withZone(ZoneId.systemDefault())
                .format(fileTime.toInstant());
    }

    private static String fileNameComplement(Path path, BasicFileAttributes attributes) throws IOException {
        if (attributes.isDirectory()) {
            return "/";
        }
        if (attributes.isSymbolicLink()) {
            return " -> " + readSymbolicLink(path).toString();
        }
        return "";
    }

    private static List<List<String>> pad(List<List<Cell>> rows) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        List<Integer> lengths = rows.get(0)
                .stream()
                .map(c -> c.value.length())
                .collect(toList());

        rows.forEach(row -> {
            range(0, row.size()).forEach(i -> {
                int maxLen = max(lengths.get(i), row.get(i).getValue().length());
                lengths.set(i, maxLen);
            });
        });

        return rows.stream()
                .map(row -> pad(row, lengths))
                .collect(toList());
    }

    private static List<String> pad(List<Cell> row, List<Integer> lengths) {
        return range(0, row.size())
                .mapToObj(i -> row.get(i).pad(lengths.get(i)))
                .collect(toList());
    }

    private static class Cell {

        private final String value;
        private final boolean leftPadded;

        public static Cell leftPadded(String value) {
            return new Cell(value, true);
        }

        public static Cell rightPadded(String value) {
            return new Cell(value, false);
        }

        private Cell(String value, boolean leftPadded) {
            this.value = value;
            this.leftPadded = leftPadded;
        }

        public String getValue() {
            return value;
        }

        public String pad(int length) {
            if (leftPadded) {
                return padding(length) + value;
            }
            return value + padding(length);
        }

        private String padding(int length) {
            StringBuilder builder = new StringBuilder();
            range(0, length - value.length()).forEach(i -> builder.append(' '));

            return builder.toString();
        }
    }
}
