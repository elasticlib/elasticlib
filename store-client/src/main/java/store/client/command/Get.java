package store.client.command;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.client.util.DefferedFileOutputStream;
import store.client.util.Directories;
import store.common.client.Content;
import store.common.client.RequestFailedException;
import store.common.hash.Hash;
import static store.common.util.IoUtil.copy;

class Get extends AbstractCommand {

    Get() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Get an existing content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        try {
            try (Content content = session.getRepository().getContent(hash)) {
                Path path = Directories.resolve(content.getFileName().or(hash.asHexadecimalString()));
                try (InputStream inputStream = content.getInputStream();
                        OutputStream outputStream = new DefferedFileOutputStream(path)) {
                    copy(inputStream, outputStream);
                }
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }
}
