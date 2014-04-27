package store.client.display;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a text-based two-dimensional printable grid.
 */
class PrintGrid {

    private final List<StringBuilder> builders = new ArrayList<>();

    public PrintGrid(int width, int height) {
        for (int y = 0; y < height; y++) {
            builders.add(new StringBuilder());
            for (int x = 0; x < width; x++) {
                builders.get(y).append(" ");
            }
        }
    }

    public int getHeight() {
        return builders.size();
    }

    public void print(int x, int y, String text) {
        StringBuilder line = builders.get(y);
        for (int i = 0; i < text.length(); i++) {
            line.setCharAt(x + i, text.charAt(i));
        }
    }

    public List<String> render() {
        return Lists.transform(builders, new Function<StringBuilder, String>() {
            @Override
            public String apply(StringBuilder builder) {
                return builder.toString().trim();
            }
        });
    }
}
