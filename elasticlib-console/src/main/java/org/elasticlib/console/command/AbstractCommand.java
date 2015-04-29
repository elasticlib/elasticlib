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

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.common.base.Splitter;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import java.util.List;

/**
 * Base class of most command implementations.
 */
public abstract class AbstractCommand implements Command {

    private final Category category;
    private final List<Type> syntax;

    /**
     * Constructor.
     *
     * @param category This command category.
     * @param syntax This command syntax.
     */
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
    public String description() {
        return "";
    }

    @Override
    public String usage() {
        StringBuilder builder = new StringBuilder()
                .append("Usage: ")
                .append(name());

        syntax.forEach(x -> {
            builder.append(" <")
                    .append(x.name().toLowerCase())
                    .append(">");
        });

        return builder.toString();
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
