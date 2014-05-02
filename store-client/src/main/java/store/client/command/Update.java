package store.client.command;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.error.YAMLException;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.file.Directories;
import store.client.http.Session;
import static store.client.util.ClientUtil.isDeleted;
import static store.client.util.ClientUtil.parseHash;
import static store.client.util.ClientUtil.revisions;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.hash.Hash;
import store.common.value.Value;
import store.common.value.ValueType;
import store.common.yaml.YamlReading;
import store.common.yaml.YamlWriting;

class Update extends AbstractCommand {

    Update() {
        super(Category.REPOSITORY, Type.HASH);
    }

    @Override
    public String description() {
        return "Update info about an existing content in current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String editor = config.getEditor();
        if (editor.isEmpty()) {
            throw new RequestFailedException("No defined editor");
        }
        String repository = session.getRepository();
        Hash hash = parseHash(params.get(0));
        List<ContentInfo> head = session.getClient().getInfoHead(repository, hash);
        if (isDeleted(head)) {
            throw new RequestFailedException("This content is deleted");
        }
        ContentInfo updated = update(editor, head);
        if (head.size() == 1 && head.get(0).getMetadata().equals(updated.getMetadata())) {
            throw new RequestFailedException("Not modified");
        }
        CommandResult result = session.getClient().update(repository, updated);
        display.print(result);
    }

    private static ContentInfo update(String editor, List<ContentInfo> head) {
        try {
            Files.createDirectories(Directories.home());
            Path tmp = Files.createTempFile(Directories.home(), "tmp", ".yml").toAbsolutePath();
            try {
                write(head, tmp);
                new ProcessBuilder(editor, tmp.toString()).start().waitFor();

                return new ContentInfoBuilder()
                        .withContent(head.get(0).getContent())
                        .withLength(head.get(0).getLength())
                        .withParents(revisions(head))
                        .withMetadata(metadata(read(tmp)))
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
        try (BufferedWriter writer = Files.newBufferedWriter(file, Charsets.UTF_8)) {
            if (head.size() == 1) {
                writer.write(YamlWriting.writeValue(Value.of(head.get(0).getMetadata())));

            } else {
                for (ContentInfo info : head) {
                    writer.write("#revision " + info.getRevision().asHexadecimalString());
                    writer.newLine();
                    writer.write(YamlWriting.writeValue(Value.of(info.getMetadata())));
                    writer.newLine();
                }
            }
        }
    }

    private static String read(Path file) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, Charsets.UTF_8)) {
            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[1024];
            int length = reader.read(buffer);
            while (length > 0) {
                builder.append(buffer, 0, length);
                length = reader.read(buffer);
            }
            return builder.toString();
        }
    }

    private static Map<String, Value> metadata(String text) {
        if (text.isEmpty()) {
            return Collections.emptyMap();
        }
        Value value = readYaml(text);
        if (value.type() != ValueType.OBJECT) {
            throw new RequestFailedException("Failed to read updated metadata");
        }
        return value.asMap();
    }

    private static Value readYaml(String text) {
        try {
            return YamlReading.readValue(text);

        } catch (YAMLException e) {
            throw new RequestFailedException(e);
        }
    }
}
