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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import org.elasticlib.console.command.Category;
import org.elasticlib.console.command.Command;
import org.elasticlib.console.command.ParametersCompleter;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import org.elasticlib.console.i18n.Resources;
import static org.elasticlib.console.util.Directories.workingDirectory;

/**
 * Adapter for native OS commands execution.
 */
public class OsCommand implements Command {

    @Override
    public String name() {
        return "!";
    }

    @Override
    public Category category() {
        return Category.MISC;
    }

    @Override
    public String summary() {
        return Resources.get("command.osCommand.summary");
    }

    @Override
    public String description() {
        return Resources.tryGet("command.osCommand.description").orElse("");
    }

    @Override
    public String usage() {
        return "Usage: !<os_command> <args>...";
    }

    @Override
    public List<String> params(List<String> argList) {
        if (argList.isEmpty()) {
            return argList;
        }
        return argList.subList(1, argList.size());
    }

    @Override
    public List<String> complete(ParametersCompleter completer, List<String> params) {
        return Collections.emptyList();
    }

    @Override
    public boolean isValid(List<String> params) {
        return !params.isEmpty();
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        try {
            Process process = new ProcessBuilder(params)
                    .directory(workingDirectory().toFile())
                    .start();

            try (BufferedReader out = reader(process.getInputStream());
                    BufferedReader err = reader(process.getErrorStream())) {

                String buffer = out.readLine();
                while (buffer != null) {
                    display.println(buffer);
                    buffer = out.readLine();
                }
                buffer = err.readLine();
                while (buffer != null) {
                    display.println(buffer);
                    buffer = err.readLine();
                }
            } finally {
                process.destroy();
            }
        } catch (IOException e) {
            display.println(e.getMessage());
        }
    }

    private static BufferedReader reader(InputStream input) {
        return new BufferedReader(new InputStreamReader(input));
    }
}
