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

import com.google.common.base.Charsets;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.String.join;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import org.elasticlib.common.yaml.YamlReader;
import org.elasticlib.common.yaml.YamlWriter;
import org.elasticlib.console.command.AbstractCommand;
import org.elasticlib.console.command.Category;
import org.elasticlib.console.command.Type;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.tokenizing.Tokenizing.argList;
import static org.elasticlib.console.util.ClientUtil.parseHash;
import static org.elasticlib.console.util.ClientUtil.revisions;
import static org.elasticlib.console.util.Directories.home;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * The update command.
 */
public class Update extends AbstractCommand {

    /**
     * Constructor.
     */
    public Update() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String summary() {
        return "Update info about an existing content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        String editor = config.getEditor();
        if (editor.isEmpty()) {
            throw new RequestFailedException("No defined editor");
        }
        Hash hash = parseHash(params.get(0));
        ContentInfo contentInfo = session.getRepository().getContentInfo(hash);
        check(contentInfo);

        List<Revision> head = contentInfo.getHead();
        Revision updated = update(editor, head);
        if (head.size() == 1 && head.get(0).getMetadata().equals(updated.getMetadata())) {
            throw new RequestFailedException("Not modified");
        }

        CommandResult result = session.getRepository().addRevision(updated);
        display.print(result);
    }

    private static void check(ContentInfo contentInfo) {
        if (contentInfo.getHead().isEmpty()) {
            throw new RequestFailedException("This content is unknown");
        }
        if (contentInfo.getState() != ContentState.PRESENT) {
            throw new RequestFailedException("This content is deleted");
        }
    }

    private static Revision update(String editor, List<Revision> head) {
        try {
            Files.createDirectories(home());
            Path tmp = Files.createTempFile(home(), "tmp", ".yml");
            try {
                write(head, tmp);
                execute(editor, tmp);

                return new RevisionBuilder()
                        .withContent(head.get(0).getContent())
                        .withLength(head.get(0).getLength())
                        .withParents(revisions(head))
                        .withMetadata(metadata(tmp))
                        .computeRevisionAndBuild();

            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private static void write(List<Revision> head, Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, Charsets.UTF_8);
                YamlWriter yamlWriter = new YamlWriter(writer)) {
            if (head.size() == 1) {
                yamlWriter.writeValue(Value.of(head.get(0).getMetadata()));

            } else {
                for (Revision info : head) {
                    writer.write("#revision " + info.getRevision().asHexadecimalString());
                    writer.newLine();
                    yamlWriter.writeValue(Value.of(info.getMetadata()));
                    writer.newLine();
                }
            }
        }
    }

    private static void execute(String editor, Path file) throws IOException {
        try {
            List<String> command = argList(join(" ", editor, file.toString()));
            new ProcessBuilder(command)
                    .start()
                    .waitFor();

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static Map<String, Value> metadata(Path file) throws IOException {
        try (InputStream input = Files.newInputStream(file);
                YamlReader reader = new YamlReader(input)) {

            Optional<Value> value = reader.readValue();
            if (!value.isPresent()) {
                return Collections.emptyMap();
            }
            if (value.get().type() != ValueType.OBJECT) {
                throw new RequestFailedException("Failed to read updated metadata");
            }
            return value.get().asMap();

        } catch (YAMLException e) {
            throw new RequestFailedException(e);
        }
    }
}
