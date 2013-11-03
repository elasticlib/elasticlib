package store.server.volume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import static org.fest.assertions.api.Assertions.assertThat;
import store.server.Content;

class GetTask extends Task {

    private final Volume volume;
    private final Content content;

    public GetTask(Volume volume, Content content) {
        this.volume = volume;
        this.content = content;
    }

    @Override
    protected void execute() {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            volume.get(content.getHash(), outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(content.getBytes());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
