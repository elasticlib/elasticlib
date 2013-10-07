package store.common;

public class Metadata {

    public static final class Common {

        public static final String FILE_NAME = "fileName";
        public static final String CONTENT_TYPE = "contentType";
        public static final String CAPTURE_DATE = "captureDate";

        private Common() {
        }
    }

    public static final class Text {

        public static final String ENCODING = "encoding";

        private Text() {
        }
    }

    public static final class Audio {

        public static final String ARTIST = "artist";
        public static final String ALBUM = "album";
        public static final String TITLE = "title";
        public static final String GENRE = "genre";
        public static final String TRACK_NUMBER = "trackNumber";
        public static final String DURATION = "duration";
        public static final String RELEASE_DATE = "releaseDate";

        private Audio() {
        }
    }

    public static final class Image {

        public static final String HEIGHT = "height";
        public static final String WIDTH = "width";
        public static final String ORIGINAL_DATE = "originalDate";

        private Image() {
        }
    }
}
