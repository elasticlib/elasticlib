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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.sort;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

final class CommandProvider {

    private static final List<Command> COMMANDS = Arrays.<Command>asList(new Config(),
                                                                         new Set(),
                                                                         new Unset(),
                                                                         new Reset(),
                                                                         new Connect(),
                                                                         new Disconnect(),
                                                                         new Node(),
                                                                         new Use(),
                                                                         new Leave(),
                                                                         new CreateRepository(),
                                                                         new CreateReplication(),
                                                                         new DropRepository(),
                                                                         new DropReplication(),
                                                                         new AddRepository(),
                                                                         new AddRemote(),
                                                                         new Open(),
                                                                         new Close(),
                                                                         new RemoveRepository(),
                                                                         new RemoveRemote(),
                                                                         new Start(),
                                                                         new Stop(),
                                                                         new Remotes(),
                                                                         new Repositories(),
                                                                         new Replications(),
                                                                         new Put(),
                                                                         new Update(),
                                                                         new Delete(),
                                                                         new Get(),
                                                                         new Revisions(),
                                                                         new Head(),
                                                                         new Tree(),
                                                                         new Find(),
                                                                         new History(),
                                                                         new Cd(),
                                                                         new Pwd(),
                                                                         new Quit(),
                                                                         new Help(),
                                                                         new OsCommand());

    static {
        sort(COMMANDS, (c1, c2) -> c1.name().compareTo(c2.name()));
    }

    private CommandProvider() {
    }

    public static Optional<Command> command(List<String> argList) {
        if (argList.isEmpty()) {
            return Optional.empty();
        }
        for (Command command : COMMANDS) {
            if (matches(command, argList)) {
                return Optional.of(command);
            }
        }
        return Optional.empty();
    }

    private static boolean matches(Command command, List<String> argList) {
        Iterator<String> parts = Splitter.on(' ').split(command.name()).iterator();
        Iterator<String> args = argList.iterator();
        while (parts.hasNext()) {
            if (!args.hasNext() || !parts.next().equals(args.next())) {
                return false;
            }
        }
        return true;
    }

    public static List<Command> commands() {
        return COMMANDS;
    }

    public static String help() {
        List<String> categoryHelps = new ArrayList<>();
        for (Category category : Category.values()) {
            StringBuilder builder = new StringBuilder();
            builder.append(category).append(System.lineSeparator());
            COMMANDS.stream()
                    .filter(command -> command.category() == category)
                    .forEach(command -> {
                        builder.append(tab(2))
                        .append(fixedSize(command.name(), 24))
                        .append(command.description())
                        .append(System.lineSeparator());
                    });
            categoryHelps.add(builder.toString());
        }
        return Joiner.on(System.lineSeparator()).join(categoryHelps);
    }

    private static String tab(int size) {
        StringBuilder builder = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            builder.append(' ');
        }
        return builder.toString();
    }

    private static String fixedSize(String value, int size) {
        StringBuilder builder = new StringBuilder(value);
        for (int i = 0; i < size - value.length(); i++) {
            builder.append(' ');
        }
        return builder.toString();
    }
}
