package store.client.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Iterables.getOnlyElement;
import com.google.common.collect.Lists;
import static com.google.common.collect.Lists.newArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import store.client.Session;
import static store.client.command.Type.VOLUME;

abstract class AbstractCommand implements Command {

    private static String USAGE = "Usage:";
    static final String VOLUME = "volume";
    static final String INDEX = "index";
    static final String REPLICATION = "replication";

    @Override
    public String name() {
        return getClass().getSimpleName().toLowerCase();
    }

    @Override
    public String usage() {
        String name = name();
        Map<String, List<Type>> syntax = syntax();
        if (syntax.isEmpty()) {
            return Joiner.on(" ").join(USAGE, name);
        }
        if (syntax.size() == 1) {
            return Joiner.on(" ").join(USAGE, name, format(getOnlyElement(syntax.values())));
        }
        StringBuilder builder = new StringBuilder();
        Iterator<Entry<String, List<Type>>> it = syntax.entrySet().iterator();
        Entry<String, List<Type>> entry = it.next();
        builder.append(Joiner.on(" ").join(USAGE,
                                           name,
                                           entry.getKey(),
                                           format(entry.getValue())));
        while (it.hasNext()) {
            entry = it.next();
            builder.append(System.lineSeparator())
                    .append(Joiner.on(" ").join("      ",
                                                name,
                                                entry.getKey(),
                                                format(entry.getValue())));
        }
        return builder.toString();
    }

    private static String format(List<Type> types) {
        StringBuilder builder = new StringBuilder();
        for (Type type : types) {
            builder.append(type.name()).append(" ");
        }
        return builder.deleteCharAt(builder.length() - 1).toString();
    }

    @Override
    public List<String> complete(Session session, List<String> params) {
        Map<String, List<Type>> syntax = syntax();
        if (syntax.isEmpty() || params.isEmpty()) {
            return emptyList();
        }
        if (syntax.size() == 1) {
            return complete(session, params, getOnlyElement(syntax.values()));
        }
        String keyword = keyword(params);
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
        Map<String, List<Type>> syntax = syntax();
        if (syntax.isEmpty()) {
            return params.isEmpty();
        }
        if (syntax.size() == 1) {
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
        String param = params.get(lastIndex);
        switch (types.get(lastIndex)) {
            case VOLUME:
                return filterStartWith(session.getRestClient().listVolumes(), param);

            case INDEX:
                return filterStartWith(session.getRestClient().listIndexes(), param);

            case COMMAND:
                return filterStartWith(Lists.transform(CommandProvider.commands(), new Function<Command, String>() {
                    @Override
                    public String apply(Command command) {
                        return command.name();
                    }
                }), param.toLowerCase());

            default:
                return emptyList();
        }
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
}
