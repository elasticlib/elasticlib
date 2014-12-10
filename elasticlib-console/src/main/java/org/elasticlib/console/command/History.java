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

import java.util.List;
import org.elasticlib.common.model.Event;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class History extends AbstractCommand {

    private static final int CHUNK_SIZE = 20;

    History() {
        super(Category.CONTENTS);
    }

    @Override
    public String description() {
        return "Print current repository history";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        long cursor = Long.MAX_VALUE;
        List<Event> events;
        do {
            events = session.getRepository().history(false, cursor, CHUNK_SIZE);
            for (Event event : events) {
                cursor = event.getSeq() - 1;
                display.print(event);
            }
        } while (events.size() >= CHUNK_SIZE && cursor > 1);
    }
}
