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

import com.google.common.base.Splitter;
import java.util.List;
import java.util.Optional;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Help extends AbstractCommand {

    Help() {
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
            display.println(CommandProvider.help());
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
}
