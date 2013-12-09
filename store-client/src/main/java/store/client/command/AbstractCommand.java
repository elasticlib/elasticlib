package store.client.command;

import static com.google.common.base.CaseFormat.*;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Lists.newArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Map;
import store.client.Session;
import static store.client.command.Type.VOLUME;

abstract class AbstractCommand implements Command {

    static final String VOLUME = "volume";
    static final String INDEX = "index";
    static final String REPLICATION = "replication";

    @Override
    public String name() {
        return UPPER_CAMEL.to(UPPER_UNDERSCORE, getClass().getSimpleName());
    }

    protected static List<String> completeImpl(Session session, List<String> args, Map<String, List<Type>> syntax) {
        if (args.isEmpty()) {
            return emptyList();
        }
        String keyword = args.get(0).toLowerCase();
        if (args.size() == 1) {
            return filterStartWith(syntax.keySet(), keyword);
        }
        List<String> params = args.subList(1, args.size());
        if (!syntax.containsKey(keyword)) {
            return emptyList();
        }
        return completeImpl(session, params, syntax.get(keyword));
    }

    protected static List<String> completeImpl(Session session, List<String> params, Type... types) {
        return completeImpl(session, params, Arrays.asList(types));
    }

    private static List<String> completeImpl(Session session, List<String> params, List<Type> types) {
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

            default:
                return emptyList();
        }
    }

    private static List<String> filterStartWith(Collection<String> collection, final String arg) {
        List<String> list = newArrayList(Iterables.filter(collection, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(arg);
            }
        }));
        if (list.size() == 1 && list.get(0).equalsIgnoreCase(arg)) {
            return singletonList(" ");
        }
        Collections.sort(list);
        return list;
    }
}
