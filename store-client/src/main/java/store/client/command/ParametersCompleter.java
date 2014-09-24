package store.client.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import javax.ws.rs.ProcessingException;
import static store.client.command.CommandProvider.commands;
import static store.client.command.Type.KEY;
import store.client.config.ClientConfig;
import store.client.discovery.DiscoveryClient;
import store.client.http.Session;
import store.client.util.Directories;
import static store.client.util.Directories.workingDirectory;
import store.common.client.RequestFailedException;
import store.common.model.IndexEntry;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.common.model.RepositoryInfo;

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
        } catch (RequestFailedException | ProcessingException e) {
            return emptyList();
        }
    }

    private static List<String> completeConfigKey(final String param) {
        return filterStartWith(ClientConfig.listKeys(), param.toLowerCase());
    }

    private List<String> completeUri(final String param) {
        List<String> list = newArrayList(filter(uris(), new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(param.toLowerCase());
            }
        }));
        Collections.sort(list);
        return list;
    }

    private List<String> uris() {
        List<String> uris = new ArrayList<>();
        for (NodeDef def : discoveryClient.nodes()) {
            for (URI uri : def.getPublishUris()) {
                uris.add(uri.toString());
            }
        }
        return uris;
    }

    private List<String> completeNode(String param) {
        Collection<String> nodes = new TreeSet<>();
        for (NodeInfo info : session.getClient().remotes().listInfos()) {
            NodeDef def = info.getDef();
            nodes.add(def.getName());
            nodes.add(def.getGuid().asHexadecimalString());
        }
        return filterStartWith(nodes, param);
    }

    private List<String> completeRepository(String param) {
        Collection<String> repositories = new TreeSet<>();
        for (RepositoryInfo info : session.getClient().repositories().listInfos()) {
            repositories.add(info.getDef().getName());
            repositories.add(info.getDef().getGuid().asHexadecimalString());
        }
        return filterStartWith(repositories, param);
    }

    private List<String> completeHash(String param) {
        if (param.isEmpty()) {
            return emptyList();
        }
        List<String> hashes = new ArrayList<>();
        for (IndexEntry entry : session.getRepository()
                .find(Joiner.on("").join("content:", param.toLowerCase(), "*"), 0, 100)) {

            hashes.add(entry.getHash().asHexadecimalString());
        }
        return filterStartWith(hashes, param);
    }

    private static List<String> completeCommand(String param) {
        List<String> commands = transform(commands(), new Function<Command, String>() {
            @Override
            public String apply(Command command) {
                return command.name();
            }
        });
        return filterStartWith(commands, param.toLowerCase());
    }

    private static List<String> filterStartWith(Collection<String> collection, final String param) {
        List<String> list = newArrayList(filter(collection, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(param);
            }
        }));
        Collections.sort(list);
        return list;
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
