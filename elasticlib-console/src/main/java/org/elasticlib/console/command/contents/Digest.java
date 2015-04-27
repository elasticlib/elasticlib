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
package org.elasticlib.console.command.contents;

import java.util.List;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.console.command.AbstractCommand;
import org.elasticlib.console.command.Category;
import org.elasticlib.console.command.Type;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.parseHash;

/**
 * The digest command.
 */
public class Digest extends AbstractCommand {

    /**
     * Constructor.
     */
    public Digest() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Compute actual hash and length of an existing content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        display.print(session.getRepository().getDigest(hash));
    }
}
