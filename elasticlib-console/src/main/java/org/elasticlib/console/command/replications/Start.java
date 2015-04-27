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
package org.elasticlib.console.command.replications;

import java.util.List;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.console.command.AbstractCommand;
import org.elasticlib.console.command.Category;
import org.elasticlib.console.command.Type;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

/**
 * The start command.
 */
public class Start extends AbstractCommand {

    /**
     * Constructor.
     */
    public Start() {
        super(Category.REPLICATIONS, Type.REPLICATION);
    }

    @Override
    public String description() {
        return "Start an existing replication";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .replications()
                .start(new Guid(params.get(0)));

        display.printOk();
    }
}
