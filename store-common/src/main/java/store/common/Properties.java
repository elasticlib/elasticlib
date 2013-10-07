package store.common;

import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Properties {

    public static List<Property> list() {
        List<Property> list = new ArrayList<>();
        list.addAll(Arrays.asList(Common.values()));
        list.addAll(Arrays.asList(Text.values()));
        list.addAll(Arrays.asList(Audio.values()));
        list.addAll(Arrays.asList(Image.values()));
        return list;
    }

    public static enum Common implements Property {

        FILE_NAME,
        CONTENT_TYPE,
        CAPTURE_DATE;

        @Override
        public String key() {
            return key(name());
        }

        @Override
        public String label() {
            return label(name());
        }
    }

    public static enum Text implements Property {

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

    public static enum Audio implements Property {

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

    public static enum Image implements Property {

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
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
                i++;
                builder.append(toUpperCase(name.charAt(i)));
            } else {
                builder.append(toLowerCase(ch));
            }
        }
        return builder.toString();
    }

    private static String label(String name) {
        StringBuilder builder = new StringBuilder();
        builder.append(name.charAt(0));
        for (int i = 1; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
                builder.append(' ');
            } else {
                builder.append(toLowerCase(ch));
            }
        }
        return builder.toString();
    }
}
