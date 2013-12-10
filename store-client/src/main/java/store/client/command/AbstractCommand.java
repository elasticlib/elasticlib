package store.client.command;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Lists.newArrayList;
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
        return getClass().getSimpleName().toLowerCase();
    }

    @Override
    public List<String> complete(Session session, List<String> params) {
        Map<String, List<Type>> syntax = syntax();
        if (syntax.isEmpty()) {
            return complete(session, params, Collections.<Type>emptyList());
        }
        if (syntax.size() == 1) {
            return complete(session, params, syntax.values().iterator().next());
        }
        if (params.isEmpty()) {
            return emptyList();
        }
        String keyword = params.get(0).toLowerCase();
        if (params.size() == 1) {
            return filterStartWith(syntax.keySet(), keyword);
        }
        if (!syntax.containsKey(keyword)) {
            return emptyList();
        }
        return complete(session, params.subList(1, params.size()), syntax.get(keyword));
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
