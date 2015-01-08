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

import static java.lang.System.lineSeparator;
import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class About extends AbstractCommand {

    About() {
        super(Category.MISC);
    }

    @Override
    public String description() {
        return "Print info about this app";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        display.println(new StringBuilder()
                .append(appInfoLine())
                .append("Copyright 2014 Guillaume Masclet").append(lineSeparator())
                .append("Licensed under the Apache License, Version 2.0").append(lineSeparator())
                .toString());
    }

    private String appInfoLine() {
        Package pkg = getClass().getPackage();
        StringBuilder builder = new StringBuilder();
        if (pkg.getImplementationTitle() != null) {
            builder.append(pkg.getImplementationTitle()).append(" ");
        }
        if (pkg.getImplementationVersion() != null) {
            builder.append(pkg.getImplementationVersion());
        }
        if (builder.length() > 0) {
            builder.append(lineSeparator());
        }
        return builder.toString();
    }
}
