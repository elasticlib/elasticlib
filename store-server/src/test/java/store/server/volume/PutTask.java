package store.server.volume;

import java.io.IOException;
import java.io.InputStream;
import store.server.Content;
import store.server.RevSpec;

class PutTask extends Task {

    private final Volume volume;
    private final Content content;

    public PutTask(Volume volume, Content content) {
        this.volume = volume;
        this.content = content;
    }

    @Override
    protected void execute() {
        try (InputStream inputStream = content.getInputStream()) {
            volume.put(content.getInfo(), inputStream, RevSpec.any());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
