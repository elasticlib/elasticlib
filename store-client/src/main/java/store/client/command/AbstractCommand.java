package store.client.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Iterables.getOnlyElement;
import com.google.common.collect.Lists;
import static com.google.common.collect.Lists.newArrayList;
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
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.ws.rs.ProcessingException;
import static store.client.command.Type.REPOSITORY;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import store.common.IndexEntry;

abstract class AbstractCommand implements Command {

    private static final List<String> URLS = Arrays.asList("127.0.0.1", "localhost");
    private static final String USAGE = "Usage:";
    static final String OK = "ok" + System.lineSeparator();
    static final String REPOSITORY = "repository";
    static final String REPLICATION = "replication";
    private final Map<String, List<Type>> syntax;

    protected AbstractCommand() {
        this(Collections.<String, List<Type>>emptyMap());
    }

    protected AbstractCommand(Type... syntax) {
        this("", asList(syntax));
    }

    protected AbstractCommand(String key, List<Type> syntax) {
        this(singletonMap(key, syntax));
    }

    protected AbstractCommand(String key1, List<Type> syntax1,
                              String key2, List<Type> syntax2) {

        Map<String, List<Type>> map = new LinkedHashMap<>();
        map.put(key1, syntax1);
        map.put(key2, syntax2);
        syntax = map;
    }

    private AbstractCommand(Map<String, List<Type>> syntax) {
        this.syntax = syntax;
    }

    @Override
    public String name() {
        return getClass().getSimpleName().toLowerCase();
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
                case URL:
                    return completeUrl(param);

                case REPOSITORY:
                    return filterStartWith(session.getClient().listRepositories(), param);

                case HASH:
                    return filterStartWith(hashes(session, param), param);

                case COMMAND:
                    return completeCommand(param);

                case PATH:
                    return completePath(param);

                default:
                    return emptyList();
            }
        } catch (RequestFailedException | ProcessingException e) {
            return emptyList();
        }
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

    private static Collection<String> hashes(Session session, String prefix) {
        if (session.getRepository() == null || prefix.isEmpty()) {
            return emptyList();
        }
        Collection<String> hashes = new ArrayList<>();
        for (IndexEntry entry : session.getClient().find(session.getRepository(),
                                                         Joiner.on("").join("content:", prefix.toLowerCase(), "*"),
                                                         0, 100)) {
            hashes.add(entry.getHash().asHexadecimalString());
        }
        return hashes;
    }

    private static List<String> filterStartWith(Collection<String> collection, final String param) {
        List<String> list = newArrayList(Iterables.filter(collection, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(param);
            }
        }));
        if (list.size() == 1 && list.get(0).equalsIgnoreCase(param)) {
            return singletonList(" ");
        }
        Collections.sort(list);
        return list;
    }

    private static List<String> completeCommand(String param) {
        return filterStartWith(Lists.transform(CommandProvider.commands(), new Function<Command, String>() {
            @Override
            public String apply(Command command) {
                return command.name();
            }
        }), param.toLowerCase());
    }

    private static List<String> completePath(String param) {
        Path path = Paths.get(param);
        Path dir = Files.isDirectory(path) ? path : path.getParent();
        if (dir == null) {
            dir = path.isAbsolute() ? path.getRoot() : Paths.get(".");
        }
        String glob = Files.isDirectory(path) ? "*" : path.getFileName().toString() + "*";
        List<String> list = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            Iterator<Path> it = stream.iterator();
            while (it.hasNext()) {
                Path candidate = it.next();
                if (candidate.startsWith(Paths.get("."))) {
                    list.add(candidate.toString().substring(2));
                } else {
                    list.add(candidate.toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return list;
    }
}
