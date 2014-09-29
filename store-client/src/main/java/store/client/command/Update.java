package store.client.command;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.error.YAMLException;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.isDeleted;
import static store.client.util.ClientUtil.parseHash;
import static store.client.util.ClientUtil.revisions;
import static store.client.util.Directories.home;
import store.common.client.RequestFailedException;
import store.common.hash.Hash;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentInfo.ContentInfoBuilder;
import store.common.value.Value;
import store.common.value.ValueType;
import store.common.yaml.YamlReader;
import store.common.yaml.YamlWriter;

class Update extends AbstractCommand {

    Update() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Update info about an existing content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String editor = config.getEditor();
        if (editor.isEmpty()) {
            throw new RequestFailedException("No defined editor");
        }
        Hash hash = parseHash(params.get(0));
        List<ContentInfo> head = session.getRepository().getInfoHead(hash);
        if (isDeleted(head)) {
            throw new RequestFailedException("This content is deleted");
        }
        ContentInfo updated = update(editor, head);
        if (head.size() == 1 && head.get(0).getMetadata().equals(updated.getMetadata())) {
            throw new RequestFailedException("Not modified");
        }
        CommandResult result = session.getRepository().addInfo(updated);
        display.print(result);
    }

    private static ContentInfo update(String editor, List<ContentInfo> head) {
        try {
            Files.createDirectories(home());
            Path tmp = Files.createTempFile(home(), "tmp", ".yml");
            try {
                write(head, tmp);
                new ProcessBuilder(editor, tmp.toString()).start().waitFor();

                return new ContentInfoBuilder()
                        .withContent(head.get(0).getContent())
                        .withLength(head.get(0).getLength())
                        .withParents(revisions(head))
                        .withMetadata(metadata(tmp))
                        .computeRevisionAndBuild();

            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private static void write(List<ContentInfo> head, Path file) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(file, Charsets.UTF_8);
                YamlWriter yamlWriter = new YamlWriter(writer)) {
            if (head.size() == 1) {
                yamlWriter.writeValue(Value.of(head.get(0).getMetadata()));

            } else {
                for (ContentInfo info : head) {
                    writer.write("#revision " + info.getRevision().asHexadecimalString());
                    writer.newLine();
                    yamlWriter.writeValue(Value.of(info.getMetadata()));
                    writer.newLine();
                }
            }
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
