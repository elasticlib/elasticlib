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
import java.util.Arrays;
import static java.util.Collections.sort;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.elasticlib.console.command.config.Config;
import org.elasticlib.console.command.config.Reset;
import org.elasticlib.console.command.config.Set;
import org.elasticlib.console.command.config.Unset;
import org.elasticlib.console.command.contents.Delete;
import org.elasticlib.console.command.contents.Digest;
import org.elasticlib.console.command.contents.Find;
import org.elasticlib.console.command.contents.Get;
import org.elasticlib.console.command.contents.Head;
import org.elasticlib.console.command.contents.History;
import org.elasticlib.console.command.contents.Put;
import org.elasticlib.console.command.contents.Revisions;
import org.elasticlib.console.command.contents.Tree;
import org.elasticlib.console.command.contents.Update;
import org.elasticlib.console.command.misc.About;
import org.elasticlib.console.command.misc.Cd;
import org.elasticlib.console.command.misc.Help;
import org.elasticlib.console.command.misc.Ls;
import org.elasticlib.console.command.misc.OsCommand;
import org.elasticlib.console.command.misc.Pwd;
import org.elasticlib.console.command.misc.Quit;
import org.elasticlib.console.command.node.Connect;
import org.elasticlib.console.command.node.Disconnect;
import org.elasticlib.console.command.node.Leave;
import org.elasticlib.console.command.node.Node;
import org.elasticlib.console.command.node.Use;
import org.elasticlib.console.command.remotes.AddRemote;
import org.elasticlib.console.command.remotes.Remotes;
import org.elasticlib.console.command.remotes.RemoveRemote;
import org.elasticlib.console.command.replications.CreateReplication;
import org.elasticlib.console.command.replications.DropReplication;
import org.elasticlib.console.command.replications.Replications;
import org.elasticlib.console.command.replications.Start;
import org.elasticlib.console.command.replications.Stop;
import org.elasticlib.console.command.repositories.AddRepository;
import org.elasticlib.console.command.repositories.Close;
import org.elasticlib.console.command.repositories.CreateRepository;
import org.elasticlib.console.command.repositories.DropRepository;
import org.elasticlib.console.command.repositories.Open;
import org.elasticlib.console.command.repositories.RemoveRepository;
import org.elasticlib.console.command.repositories.Repositories;

/**
 * Provides commands.
 */
public final class CommandProvider {

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
                                                                         new Digest(),
                                                                         new Revisions(),
                                                                         new Head(),
                                                                         new Tree(),
                                                                         new Find(),
                                                                         new History(),
                                                                         new About(),
                                                                         new Cd(),
                                                                         new Ls(),
                                                                         new Pwd(),
                                                                         new Quit(),
                                                                         new Help(),
                                                                         new OsCommand());

    static {
        sort(COMMANDS, (c1, c2) -> c1.name().compareTo(c2.name()));
    }

    private CommandProvider() {
    }

    /**
     * Provides command matching supplied arguments list, if any.
     *
     * @param argList An arguments list.
     * @return Corresponding command, if any.
     */
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

    /**
     * Lists all commands.
     *
     * @return A list of commands.
     */
    public static List<Command> commands() {
        return COMMANDS;
    }
}
