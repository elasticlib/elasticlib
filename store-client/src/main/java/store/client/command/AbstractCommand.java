package store.client.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getOnlyElement;
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
import java.util.Map;
import java.util.Map.Entry;
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
import store.common.RepositoryDef;

abstract class AbstractCommand implements Command {

    private static final List<String> URLS = Arrays.asList("127.0.0.1", "localhost");
    private static final String USAGE = "Usage:";
    static final String OK = "ok" + System.lineSeparator();
    static final String REPOSITORY = "repository";
    static final String REPLICATION = "replication";
    private final Category category;
    private final Map<String, List<Type>> syntax;

    protected AbstractCommand(Category category) {
        this(category, Collections.<String, List<Type>>emptyMap());
    }

    protected AbstractCommand(Category category, Type... syntax) {
        this(category, ImmutableMap.of("", asList(syntax)));
    }

    protected AbstractCommand(Category category, String key, List<Type> syntax) {
        this(category, ImmutableMap.of(key, syntax));
    }

    protected AbstractCommand(Category category,
                              String key1, List<Type> syntax1,
                              String key2, List<Type> syntax2) {

        this(category, ImmutableMap.of(key1, syntax1, key2, syntax2));
    }

    private AbstractCommand(Category category, Map<String, List<Type>> syntax) {
        this.category = category;
        this.syntax = syntax;
    }

    @Override
    public String name() {
        return getClass().getSimpleName().toLowerCase();
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
        StringBuilder builder = new StringBuilder();
        Iterator<Entry<String, List<Type>>> it = syntax.entrySet().iterator();
        Entry<String, List<Type>> entry = it.next();
        builder.append(Joiner.on(" ").join(USAGE, name, format(entry)));
        while (it.hasNext()) {
            entry = it.next();
            builder.append(System.lineSeparator())
                    .append(Joiner.on(" ").join("      ", name, format(entry)));
        }
        return builder.toString();
    }

    private static String format(Entry<String, List<Type>> entry) {
        StringBuilder builder = new StringBuilder();
        if (!entry.getKey().isEmpty()) {
            builder.append(entry.getKey()).append(" ");
        }
        for (Type type : entry.getValue()) {
            builder.append(type.name()).append(" ");
        }
        return builder.deleteCharAt(builder.length() - 1).toString();
    }

    @Override
    public List<String> complete(Session session, List<String> params) {
        if (syntax.isEmpty() || params.isEmpty()) {
            return emptyList();
        }
        if (syntax.size() == 1 && getOnlyElement(syntax.keySet()).isEmpty()) {
            return complete(session, params, getOnlyElement(syntax.values()));
        }
        return complete(session, keyword(params), params);
    }

    private List<String> complete(Session session, String keyword, List<String> params) {
        if (params.size() == 1) {
            return filterStartWith(syntax.keySet(), keyword);
        }
        if (!syntax.containsKey(keyword)) {
            return emptyList();
        }
        return complete(session, params.subList(1, params.size()), syntax.get(keyword));
    }

    @Override
    public boolean isValid(List<String> params) {
        if (syntax.isEmpty()) {
            return params.isEmpty();
        }
        if (syntax.size() == 1 && getOnlyElement(syntax.keySet()).isEmpty()) {
            return params.size() == getOnlyElement(syntax.values()).size();
        }
        if (params.isEmpty() || !syntax.containsKey(keyword(params))) {
            return false;
        }
        return params.size() == syntax.get(keyword(params)).size() + 1;
    }

    private static String keyword(List<String> params) {
        return params.get(0).toLowerCase();
    }

    private static List<String> complete(Session session, List<String> params, List<Type> types) {
        if (params.size() > types.size()) {
            return emptyList();
        }
        int lastIndex = params.size() - 1;
        return complete(session, params.get(lastIndex), types.get(lastIndex));
    }

    private static List<String> complete(Session session, String param, Type type) {
        try {
            switch (type) {
                case KEY:
                    return completeConfigKey(param);

                case URL:
                    return completeUrl(param);

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

    private static List<String> completeUrl(final String param) {
        List<String> list = newArrayList(Iterables.filter(URLS, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(param.toLowerCase());
            }
        }));
        Collections.sort(list);
        return list;
    }

    private static List<String> completeRepository(Session session, String param) {
        Collection<String> repositories = new TreeSet<>();
        for (RepositoryDef def : session.getClient().listRepositoryDefs()) {
            repositories.add(def.getName());
            repositories.add(def.getGuid().asHexadecimalString());
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
