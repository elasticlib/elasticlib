package store.client.command;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Arrays.asList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import javax.ws.rs.ProcessingException;
import static store.client.command.CommandProvider.commands;
import static store.client.command.Type.REPOSITORY;
import store.client.config.ClientConfig;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import store.client.util.Directories;
import static store.client.util.Directories.workingDirectory;
import store.common.IndexEntry;
import store.common.NodeDef;
import store.common.RepositoryInfo;

abstract class AbstractCommand implements Command {

    private static final List<String> URIS = Arrays.asList("127.0.0.1", "localhost");
    private static final String USAGE = "Usage:";
    private final Category category;
    private final List<Type> syntax;

    protected AbstractCommand(Category category, Type... syntax) {
        this.category = category;
        this.syntax = asList(syntax);
    }

    @Override
    public String name() {
        return UPPER_CAMEL.to(LOWER_UNDERSCORE, getClass().getSimpleName()).replace('_', ' ');
    }

    @Override
    public Category category() {
        return category;
    }

    @Override
    public String usage() {
        String name = name();
        if (syntax.isEmpty()) {
            return Joiner.on(" ").join(USAGE, name);
        }
        return Joiner.on(" ").join(USAGE, name, Joiner.on(" ").join(syntax));
    }

    @Override
    public List<String> params(List<String> argList) {
        int parts = Splitter.on(' ').splitToList(name()).size();
        if (argList.size() < parts) {
            return emptyList();
        }
        return argList.subList(parts, argList.size());
    }

    @Override
    public boolean isValid(List<String> params) {
        return params.size() == syntax.size();
    }

    @Override
    public List<String> complete(Session session, List<String> params) {
        if (params.isEmpty() || params.size() > syntax.size()) {
            return emptyList();
        }
        int lastIndex = params.size() - 1;
        return complete(session, params.get(lastIndex), syntax.get(lastIndex));
    }

    private static List<String> complete(Session session, String param, Type type) {
        try {
            switch (type) {
                case KEY:
                    return completeConfigKey(param);

                case URI:
                    return completeUri(param);

                case NODE:
                    return completeNode(session, param);

                case REPOSITORY:
                    return completeRepository(session, param);

                case HASH:
                    return completeHash(session, param);

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

    private static List<String> completeUri(final String param) {
        List<String> list = newArrayList(Iterables.filter(URIS, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(param.toLowerCase());
            }
        }));
        Collections.sort(list);
        return list;
    }

    private static List<String> completeNode(Session session, String param) {
        Collection<String> nodes = new TreeSet<>();
        for (NodeDef def : session.getClient().listRemotes()) {
            nodes.add(def.getName());
            nodes.add(def.getGuid().asHexadecimalString());
        }
        return filterStartWith(nodes, param);
    }

    private static List<String> completeRepository(Session session, String param) {
        Collection<String> repositories = new TreeSet<>();
        for (RepositoryInfo info : session.getClient().listRepositoryInfos()) {
            repositories.add(info.getDef().getName());
            repositories.add(info.getDef().getGuid().asHexadecimalString());
        }
        return filterStartWith(repositories, param);
    }

    private static List<String> completeHash(Session session, String param) {
        if (param.isEmpty()) {
            return emptyList();
        }
        List<String> hashes = new ArrayList<>();
        for (IndexEntry entry : session.getClient().find(session.getRepository(),
                                                         Joiner.on("").join("content:", param.toLowerCase(), "*"),
                                                         0, 100)) {
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
        Path path = Directories.resolve(Paths.get(param));
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
