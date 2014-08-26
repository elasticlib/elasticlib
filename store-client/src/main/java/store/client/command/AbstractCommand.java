package store.client.command;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import java.util.List;

abstract class AbstractCommand implements Command {

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
    public List<String> complete(ParametersCompleter completer, List<String> params) {
        if (params.isEmpty() || params.size() > syntax.size()) {
            return emptyList();
        }
        int lastIndex = params.size() - 1;
        return completer.complete(params.get(lastIndex), syntax.get(lastIndex));
    }
}
