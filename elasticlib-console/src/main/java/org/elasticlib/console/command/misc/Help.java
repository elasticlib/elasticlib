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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
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
        super(Category.MISC, Type.COMMAND);
    }

    @Override
    public String description() {
        return "Print help about a command";
    }

    @Override
    public boolean isValid(List<String> params) {
        return true;
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Optional<Command> commandOpt = command(params);
        if (!commandOpt.isPresent()) {
            display.println(generalHelp());
            return;
        }
        Command command = commandOpt.get();
        display.println(new StringBuilder()
                .append(command.description())
                .append(System.lineSeparator())
                .append(command.usage())
                .append(System.lineSeparator())
                .toString());
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

    private static String generalHelp() {
        List<String> categoryHelps = new ArrayList<>();
        for (Category category : Category.values()) {
            StringBuilder builder = new StringBuilder();
            builder.append(category).append(System.lineSeparator());
            CommandProvider.commands()
                    .stream()
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
