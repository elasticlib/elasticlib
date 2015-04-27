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

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

/**
 * A command.
 */
public interface Command {

    /**
     * @return This command name.
     */
    String name();

    /**
     * @return This command category.
     */
    Category category();

    /**
     * @return Description of this command.
     */
    String description();

    /**
     *
     * @return Human readable command syntax.
     */
    String usage();

    /**
     * Extract parameters list from supplied command line argument list (ie truncate command name).
     *
     * @param argList Command line argument list.
     * @return Corresponding parameters list.
     */
    List<String> params(List<String> argList);

    /**
     * Complete the supplied parameters list.
     *
     * @param completer Parameters completer.
     * @param params Parameters (Exclude command name).
     * @return A list of candidates for completion.
     */
    List<String> complete(ParametersCompleter completer, List<String> params);

    /**
     * Validate the supplied parameters list.
     *
     * @param params Parameters (Exclude command name).
     * @return True if parameters list matches command syntax.
     */
    boolean isValid(List<String> params);

    /**
     * Execute the command.
     *
     * @param display Display to output to.
     * @param session Session to execute against.
     * @param config Console configuration.
     * @param params Parameters (Exclude command name).
     */
    void execute(Display display, Session session, ConsoleConfig config, List<String> params);
}
