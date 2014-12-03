package store.common.metadata;

import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(MetadataUtil.class);

    private MetadataUtil() {
    }

    /**
     * Extracts metadata from file the located at supplied path.
     *
     * @param filepath File path.
     * @param inputStream Input-stream on the file.
     * @return Extracted metadata as a map of Values.
     * @throws IOException If an I/O error occurs.
     */
    public static Map<String, Value> metadata(Path filepath, InputStream inputStream) throws IOException {
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, filepath.getFileName().toString());

        try {
            new AutoDetectParser().parse(inputStream, new DefaultHandler(), metadata, new ParseContext());

        } catch (SAXException | TikaException e) {
            LOG.error("Failed to fully extract metadata from " + filepath.toAbsolutePath(), e);
        }
        return Extractor.extract(metadata);
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
            String raw = metadata.get(tikaKey);

            // Bug in Tika 1.5 : Metadata.getDate() is likely to crash if string length is lower than 6 :
            // it calls Metadata.parseDate(String) which does not check supplied input length.
            if (raw.length() < 6) {
                return Value.of(raw);
            }
            Date date = metadata.getDate(tikaKey);
            if (date != null) {
                return Value.of(date.toInstant());
            }
            return Value.of(raw);
        }

        private void convert(String tikaKey, Property property) {
            String value = metadata.get(tikaKey);
            if (value != null) {
                map.put(property.key(), Value.of(metadata.get(tikaKey)));
            }
        }
    }
}
