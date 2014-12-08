package org.elasticlib.node.multipart;

import java.io.InputStream;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.jvnet.mimepull.MIMEPart;

/**
 * The Body part of a multipart entity. Intended to be read-only and read-once. Failure will occur if body content is
 * accessed twice (via any method).
 */
public class BodyPart {

    private final MIMEPart mimePart;
    private final Optional<ContentDisposition> contentDisposition;

    BodyPart(MIMEPart mimePart, ContentDisposition contentDisposition) {
        this.mimePart = mimePart;
        this.contentDisposition = Optional.ofNullable(contentDisposition);
    }

    /**
     * @return The content disposition of this part, if any.
     */
    public Optional<ContentDisposition> getContentDisposition() {
        return contentDisposition;
    }

    /**
     * @return An inputStream on the content.
     */
    public InputStream getAsInputStream() {
        return new MimePartInputStream(mimePart);
    }

    /**
     * @return This content as a JSON structure.
     */
    public JsonStructure getAsJsonStructure() {
        try (JsonReader reader = Json.createReaderFactory(null).createReader(getAsInputStream())) {
            return reader.read();
        }
    }

    /**
     * @return This content as a JSON object.
     */
    public JsonObject getAsJsonObject() {
        try (JsonReader reader = Json.createReaderFactory(null).createReader(getAsInputStream())) {
            return reader.readObject();
        }
    }

    /**
     * @return This content as a JSON array.
     */
    public JsonArray getAsJsonArray() {
        try (JsonReader reader = Json.createReaderFactory(null).createReader(getAsInputStream())) {
            return reader.readArray();
        }
    }
}
