package store.server.volume;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import store.server.Content;
import store.server.RevSpec;

class PutWithFailureTask extends Task {

    private final Volume volume;
    private final Content content;

    public PutWithFailureTask(Volume volume, Content content) {
        this.volume = volume;
        this.content = content;
    }

    @Override
    protected void execute() {
        try (InputStream inputStream = new ByteArrayInputStream(content.getBytes(), 0, content.getBytes().length / 2)) {
            volume.put(content.getInfo(), inputStream, RevSpec.any());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
