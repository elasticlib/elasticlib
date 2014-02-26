package store.common.metadata;

import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TIFF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.XMPDM;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import store.common.metadata.Properties.Audio;
import store.common.metadata.Properties.Common;
import store.common.metadata.Properties.Image;
import store.common.metadata.Properties.Text;
import store.common.value.Value;

/**
 * Metadata extraction utils.
 */
public final class MetadataUtil {

    private MetadataUtil() {
    }

    /**
     * Extracts metadata from file the located at supplied path.
     *
     * @param filepath File path.
     * @return Extracted metadata as a map of Values.
     * @throws IOException If an I/O error occurs.
     */
    public static Map<String, Value> metadata(Path filepath) throws IOException {
        try (InputStream inputStream = Files.newInputStream(filepath)) {
            Metadata metadata = new Metadata();
            metadata.set(Metadata.RESOURCE_NAME_KEY, filepath.getFileName().toString());
            new AutoDetectParser().parse(inputStream, new DefaultHandler(), metadata, new ParseContext());
            return Extractor.extract(metadata);

        } catch (SAXException | TikaException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Extractor {

        private final Map<String, Value> map = new HashMap<>();
        private final Metadata metadata;

        private Extractor(Metadata metadata) {
            this.metadata = metadata;
        }

        public static Map<String, Value> extract(Metadata metadata) {
            Extractor extractor = new Extractor(metadata);
            extractor.convert(Metadata.RESOURCE_NAME_KEY, Common.FILE_NAME);
            extractor.convert(Metadata.CONTENT_TYPE, Common.CONTENT_TYPE);

            String type = Splitter.on('/').split(metadata.get(Metadata.CONTENT_TYPE)).iterator().next();
            switch (type) {
                case "text":
                    extractor.convertText();
                    break;
                case "audio":
                    extractor.convertAudio();
                    break;
                case "image":
                    extractor.convertImage();
                    break;
                default:
                    break;
            }
            return extractor.map;
        }

        private void convertText() {
            convert(Metadata.CONTENT_ENCODING, Text.ENCODING);
        }

        private void convertAudio() {
            convert(XMPDM.ARTIST, Audio.ARTIST);
            convert(XMPDM.ALBUM, Audio.ALBUM);
            convert(TikaCoreProperties.TITLE, Audio.TITLE);
            convert(XMPDM.GENRE, Audio.GENRE);
            convert(XMPDM.TRACK_NUMBER, Audio.TRACK_NUMBER);
            convert(XMPDM.DURATION, Audio.DURATION);
            convert(XMPDM.RELEASE_DATE, Audio.RELEASE_DATE);
        }

        private void convertImage() {
            convert(TIFF.IMAGE_LENGTH, Image.HEIGHT);
            convert(TIFF.IMAGE_WIDTH, Image.WIDTH);
            convert(TIFF.ORIGINAL_DATE, Image.ORIGINAL_DATE);

        }

        private void convert(org.apache.tika.metadata.Property tikaKey, Property property) {
            if (metadata.get(tikaKey) == null) {
                return;
            }
            switch (tikaKey.getValueType()) {
                case BOOLEAN:
                    map.put(property.key(), Value.of(Boolean.parseBoolean(metadata.get(tikaKey))));
                    break;

                case INTEGER:
                    map.put(property.key(), getInt(tikaKey));
                    break;

                case REAL:
                    map.put(property.key(), Value.of(new BigDecimal(metadata.get(tikaKey))));
                    break;

                case DATE:
                    map.put(property.key(), getDate(tikaKey));
                    break;

                default:
                    map.put(property.key(), Value.of(metadata.get(tikaKey)));
                    break;
            }
        }

        private Value getInt(org.apache.tika.metadata.Property tikaKey) {
            Integer i = metadata.getInt(tikaKey);
            if (i != null) {
                return Value.of(i);
            }
            return Value.of(metadata.get(tikaKey));
        }

        private Value getDate(org.apache.tika.metadata.Property tikaKey) {
            Date date = metadata.getDate(tikaKey);
            if (date != null) {
                return Value.of(date);
            }
            return Value.of(metadata.get(tikaKey));
        }

        private void convert(String tikaKey, Property property) {
            String value = metadata.get(tikaKey);
            if (value != null) {
                map.put(property.key(), Value.of(metadata.get(tikaKey)));
            }
        }
    }
}
