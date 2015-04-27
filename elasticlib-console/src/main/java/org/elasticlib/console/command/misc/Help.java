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
package org.elasticlib.console.command.misc;

import com.google.common.base.Splitter;
import static java.lang.System.lineSeparator;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.elasticlib.console.command.AbstractCommand;
import org.elasticlib.console.command.Category;
import org.elasticlib.console.command.Command;
import org.elasticlib.console.command.CommandProvider;
import org.elasticlib.console.command.Type;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

/**
 * The help command.
 */
public class Help extends AbstractCommand {

    /**
     * Constructor.
     */
    public Help() {
        super(Category.MISC, Type.SUBJECT);
    }

    @Override
    public String description() {
        return "Print help about a command or a category of commands";
    }

    @Override
    public boolean isValid(List<String> params) {
        return true;
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Optional<Category> category = category(params);
        if (category.isPresent()) {
            display.println(help(category.get()));
            return;
        }

        Optional<Command> command = command(params);
        if (command.isPresent()) {
            display.println(help(command.get()));
            return;
        }

        display.println(String.join(lineSeparator(),
                                    "Type: 'help @<category>' to list commands in <category>",
                                    "      'help <command>' to display help about <command>",
                                    "      'help <tab>' to display available help subjects",
                                    ""));
    }

    private Optional<Category> category(List<String> params) {
        if (params.isEmpty() || !params.get(0).startsWith("@")) {
            return Optional.empty();
        }
        return Arrays.stream(Category.values())
                .filter(x -> x.name().equalsIgnoreCase(params.get(0).substring(1)))
                .findFirst();
    }

    private String help(Category category) {
        StringBuilder builder = new StringBuilder()
                .append(category.getDescription())
                .append(lineSeparator())
                .append(lineSeparator());

        CommandProvider.commands()
                .stream()
                .filter(command -> command.category() == category)
                .forEach(command -> {
                    builder.append(tab(2))
                    .append(fixedSize(command.name(), 24))
                    .append(command.description())
                    .append(lineSeparator());
                });

        return builder.toString();
    }

    private Optional<Command> command(List<String> params) {
        if (params.isEmpty()) {
            return Optional.empty();
        }
        return CommandProvider.command(Splitter
                .on(' ')
                .omitEmptyStrings()
                .trimResults()
                .splitToList(params.get(0)));
    }

    private String help(Command command) {
        return new StringBuilder()
                .append(command.description())
                .append(lineSeparator())
                .append(command.usage())
                .append(lineSeparator())
                .toString();
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
