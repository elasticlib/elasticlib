package store.client.display;

import java.util.ArrayList;
import java.util.List;
import static java.util.stream.Collectors.toList;

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
        return builders.stream()
                .map(x -> x.toString().trim())
                .collect(toList());
    }
}
