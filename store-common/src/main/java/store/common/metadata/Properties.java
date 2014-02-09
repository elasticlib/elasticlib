package store.common.metadata;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Enumerates properties.
 */
public final class Properties {

    private Properties() {
    }

    /**
     * List existing properties.
     *
     * @return All properties.
     */
    public static List<Property> list() {
        List<Property> list = new ArrayList<>();
        list.addAll(Arrays.asList(Common.values()));
        list.addAll(Arrays.asList(Text.values()));
        list.addAll(Arrays.asList(Audio.values()));
        list.addAll(Arrays.asList(Image.values()));
        return list;
    }

    static enum Common implements Property {

        FILE_NAME,
        CONTENT_TYPE;

        @Override
        public String key() {
            return key(name());
        }

        @Override
        public String label() {
            return label(name());
        }
    }

    static enum Text implements Property {

        ENCODING;

        @Override
        public String key() {
            return key(name());
        }

        @Override
        public String label() {
            return label(name());
        }
    }

    static enum Audio implements Property {

        ARTIST,
        ALBUM,
        TITLE,
        GENRE,
        TRACK_NUMBER,
        DURATION,
        RELEASE_DATE;

        @Override
        public String key() {
            return key(name());
        }

        @Override
        public String label() {
            return label(name());
        }
    }

    static enum Image implements Property {

        HEIGHT,
        WIDTH,
        ORIGINAL_DATE;

        @Override
        public String key() {
            return key(name());
        }

        @Override
        public String label() {
            return label(name());
        }
    }

    private static String key(String name) {
        return UPPER_UNDERSCORE.to(LOWER_CAMEL, name);
    }

    private static String label(String name) {
        return name.charAt(0) + name.substring(1).toLowerCase().replaceAll("_", " ");
    }
}