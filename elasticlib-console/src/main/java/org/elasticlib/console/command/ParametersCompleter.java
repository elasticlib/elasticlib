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
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import static java.util.stream.Collectors.toList;
import javax.ws.rs.ProcessingException;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.model.RepositoryDef;
import static org.elasticlib.console.command.CommandProvider.commands;
import static org.elasticlib.console.command.Type.KEY;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.discovery.DiscoveryClient;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import org.elasticlib.console.util.Directories;
import static org.elasticlib.console.util.Directories.workingDirectory;

/**
 * Provides parameters completion.
 */
class ParametersCompleter {

    private final Session session;
    private final DiscoveryClient discoveryClient;

    /**
     * Constructor.
     *
     * @param session Session to inject.
     * @param discoveryClient Discovery client to inject.
     */
    public ParametersCompleter(Session session, DiscoveryClient discoveryClient) {
        this.session = session;
        this.discoveryClient = discoveryClient;
    }

    /**
     * Provides completions for the supplied parameter and type.
     *
     * @param param Parameter to complete.
     * @param type Type of this parameter.
     * @return Corresponding completion candidates.
     */
    public List<String> complete(String param, Type type) {
        try {
            switch (type) {
                case KEY:
                    return completeConfigKey(param);

                case URI:
                    return completeUri(param);

                case NODE:
                    return completeNode(param);

                case REPOSITORY:
                    return completeRepository(param);

                case HASH:
                    return completeHash(param);

                case COMMAND:
                    return completeCommand(param);

                case PATH:
                    return completePath(param, false);

                case DIRECTORY:
                    return completePath(param, true);

                default:
                    return emptyList();
            }
        } catch (NodeException | RequestFailedException | ProcessingException e) {
            return emptyList();
        }
    }

    private static List<String> completeConfigKey(final String param) {
        return filterStartWith(ConsoleConfig.listKeys(), param.toLowerCase());
    }

    private List<String> completeUri(final String param) {
        return discoveryClient.uris()
                .stream()
                .map(URI::toString)
                .filter(x -> x.startsWith(param.toLowerCase()))
                .sorted()
                .collect(toList());
    }

    private List<String> completeNode(String param) {
        Collection<String> nodes = new TreeSet<>();
        session.getClient().remotes().listInfos().forEach(info -> {
            nodes.add(info.getName());
            nodes.add(info.getGuid().asHexadecimalString());
        });
        return filterStartWith(nodes, param);
    }

    private List<String> completeRepository(String param) {
        Collection<String> repositories = new TreeSet<>();
        session.getClient().repositories().listInfos().forEach(info -> {
            RepositoryDef def = info.getDef();
            repositories.add(def.getName());
            repositories.add(def.getGuid().asHexadecimalString());
        });
        return filterStartWith(repositories, param);
    }

    private List<String> completeHash(String param) {
        if (param.isEmpty()) {
            return emptyList();
        }
        String query = String.join("", "content:", param.toLowerCase(), "*");
        List<String> hashes = session.getRepository()
                .find(query, 0, 100)
                .stream()
                .map(e -> e.getHash().asHexadecimalString())
                .collect(toList());

        return filterStartWith(hashes, param);
    }

    private static List<String> completeCommand(String param) {
        List<String> commands = commands()
                .stream()
                .map(Command::name)
                .collect(toList());

        return filterStartWith(commands, param.toLowerCase());
    }

    private static List<String> filterStartWith(Collection<String> collection, final String param) {
        return collection.stream()
                .filter(x -> x.startsWith(param))
                .sorted()
                .collect(toList());
    }

    private static List<String> completePath(String param, boolean directoriesOnly) {
        List<String> list = new ArrayList<>();
        try (DirectoryStream<Path> stream = directoryStream(param)) {
            Iterator<Path> it = stream.iterator();
            while (it.hasNext()) {
                Path candidate = it.next();
                if (directoriesOnly && !Files.isDirectory(candidate)) {
                    continue;
                }
                list.add(asString(candidate));
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
        return list;
    }

    private static DirectoryStream<Path> directoryStream(String param) throws IOException {
        Path path = Directories.resolve(param);
        Path dir = Files.isDirectory(path) ? path : path.getParent();
        if (dir == null) {
            dir = path.getRoot();
        }
        String glob = Files.isDirectory(path) ? "*" : path.getFileName().toString() + "*";
        return Files.newDirectoryStream(dir, glob);
    }

    private static String asString(Path candidate) {
        if (!candidate.startsWith(workingDirectory())) {
            return candidate.toString();
        }
        return workingDirectory().relativize(candidate).toString();
    }
}
