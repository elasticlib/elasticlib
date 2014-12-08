package org.elasticlib.console.command;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import org.elasticlib.common.client.Content;
import org.elasticlib.common.hash.Hash;
import static org.elasticlib.common.util.IoUtil.copy;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.display.LoggingInputStream;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.parseHash;
import org.elasticlib.console.util.DefferedFileOutputStream;
import org.elasticlib.console.util.Directories;

class Get extends AbstractCommand {

    Get() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Get an existing content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        try {
            try (Content content = session.getRepository().getContent(hash)) {
                Path path = Directories.resolve(content.getFileName().orElse(hash.asHexadecimalString()));
                try (InputStream inputStream = new LoggingInputStream(display,
                                                                      config,
                                                                      "Downloading",
                                                                      content.getInputStream(),
                                                                      content.getLength());
                        OutputStream outputStream = new DefferedFileOutputStream(path)) {

                    copy(inputStream, outputStream);
                }
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }
}
