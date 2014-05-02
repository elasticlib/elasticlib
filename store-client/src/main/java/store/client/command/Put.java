package store.client.command;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import store.client.util.Directories;
import store.common.CommandResult;
import store.common.metadata.Properties.Common;
import store.common.value.Value;

class Put extends AbstractCommand {

    Put() {
        super(Category.REPOSITORY, Type.PATH);
    }

    @Override
    public String description() {
        return "Put a new content in current repository";
    }

    @Override
    public void execute(final Display display, final Session session, ClientConfig config, List<String> params) {
        final String repository = session.getRepository();
        final Path path = Directories.resolve(Paths.get(params.get(0)));
        if (!Files.exists(path)) {
            throw new RequestFailedException("Does not exist");
        }
        if (!Files.isDirectory(path)) {
            CommandResult result = session.getClient().put(repository, path, Collections.<String, Value>emptyMap());
            display.print(result);
            return;
        }
        try {
            Files.walkFileTree(path,
                               EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                               Integer.MAX_VALUE,
                               new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    if (attributes.isRegularFile()) {
                        Path relative = path.getParent().relativize(file);
                        display.println("Processing: " + relative);
                        try {
                            Map<String, Value> metadata = ImmutableMap.of(Common.PATH.key(),
                                                                          Value.of(relative.getParent().toString()));
                            CommandResult result = session.getClient().put(repository, file, metadata);
                            display.print(result);

                        } catch (RequestFailedException e) {
                            display.print(e);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }
}
