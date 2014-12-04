package store.client.display;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import store.common.hash.Hash;
import store.common.model.Revision;

/**
 * Draws a text-based representation of a revision tree.
 */
class TreePrinter {

    private static final String STRAIGHT_EDGE = "| ";
    private static final String MERGE_EDGE = "|\\ ";
    private static final String RIGHT_EDGE = "\\ ";
    private static final String LEFT_EDGE = "/";
    private static final String SPACE = " ";
    private static final String DOUBLE_SPACE = "  ";
    private static final String REVISION = "*";
    private static final String DASH = "-";
    private static final String DOT = ".";

    private final int padding;
    private final Function<Revision, String> formatter;
    private final StringBuilder builder = new StringBuilder();
    private final List<Hash> previousBranches = new ArrayList<>();
    private final List<Hash> nextBranches = new ArrayList<>();
    private Revision current;
    private int cursor;

    /**
     * Constructor.
     *
     * @param padding Padding between tree and revisions.
     * @param formatter Used to format revisions.
     */
    public TreePrinter(int padding, Function<Revision, String> formatter) {
        this.padding = padding;
        this.formatter = formatter;
    }

    /**
     * Adds a revision to drawed tree. Revisions are expected to be supplied in topological order.
     *
     * @param revision Next revision to draw.
     */
    public void add(Revision revision) {
        updateMapping(revision);
        printPreRevisionLines();
        printRevisionLines();
        cleanMapping();
        printPostRevisionLines();
    }

    /**
     * Prints tree.
     *
     * @return A text-based representation of all previously supplied revisions.
     */
    public String print() {
        return builder.toString();
    }

    private void updateMapping(Revision revision) {
        current = revision;
        previousBranches.clear();
        previousBranches.addAll(nextBranches);
        nextBranches.clear();
        previousBranches.forEach(rev -> {
            if (rev.equals(current.getRevision())) {
                if (current.getParents().isEmpty()) {
                    nextBranches.add(null);
                } else {
                    addCurrentParents();
                }
            } else {
                nextBranches.add(rev);
            }
        });
        if (!previousBranches.contains(current.getRevision())) {
            addCurrentParents();
        }
    }

    private void addCurrentParents() {
        Set<Hash> parents = current.getParents();
        Set<Hash> known = Sets.intersection(parents, new HashSet<>(nextBranches));
        Set<Hash> unknown = Sets.difference(parents, known);
        nextBranches.addAll(known);
        nextBranches.addAll(unknown);
    }

    private void printPreRevisionLines() {
        if (previousBranches.size() <= 1) {
            return;
        }
        int lines = 2 * (current.getParents().size() - 2);
        boolean currentPrinted = false;
        for (int i = 0; i < lines; i++) {
            for (Hash rev : previousBranches) {
                if (rev.equals(current.getRevision())) {
                    append(STRAIGHT_EDGE);
                    pad(SPACE, i);
                    currentPrinted = true;
                } else {
                    printEdge(currentPrinted);
                }
            }
            newLine();
        }
    }

    private void printRevisionLines() {
        int width = nextBranches.isEmpty() ? 1 + padding : nextBranches.size() * 2 - 1 + padding;
        int parents = current.getParents().size();
        Iterator<String> revision = formatRevision();

        printRevisionLine(width);
        if (revision.hasNext()) {
            append(revision.next());
        }
        newLine();
        if (parents >= 2) {
            printMergeLine(parents, width);
            if (revision.hasNext()) {
                append(revision.next());
            }
            newLine();
        }
        while (revision.hasNext()) {
            printPaddingLine(width);
            append(revision.next());
            newLine();
        }
    }

    private Iterator<String> formatRevision() {
        return Splitter
                .on(System.lineSeparator())
                .split(formatter.apply(current))
                .iterator();
    }

    private void printRevisionLine(int width) {
        boolean currentPrinted = false;
        for (Hash rev : previousBranches) {
            if (rev.equals(current.getRevision())) {
                printRevision();
                currentPrinted = true;
            } else {
                printEdge(currentPrinted && current.getParents().size() > 2);
            }
        }
        if (!currentPrinted) {
            printRevision();
        }
        pad(SPACE, width - cursor);
    }

    private void printMergeLine(int parents, int width) {
        boolean currentPrinted = false;
        for (Hash rev : previousBranches) {
            if (rev.equals(current.getRevision())) {
                printMergeEdges(parents);
                currentPrinted = true;
            } else {
                printEdge(currentPrinted);
            }
        }
        if (!currentPrinted) {
            printMergeEdges(parents);
        }
        pad(SPACE, width - cursor);
    }

    private void printPaddingLine(int width) {
        nextBranches.forEach(rev -> {
            append(rev != null ? STRAIGHT_EDGE : DOUBLE_SPACE);
        });
        pad(SPACE, width - cursor);
    }

    private void printRevision() {
        int parents = current.getParents().size();
        append(REVISION);
        if (parents > 2) {
            pad(DASH, 1 + 2 * (parents - 3));
            append(DOT);
        }
        append(SPACE);
    }

    private void printEdge(boolean isSkew) {
        if (isSkew) {
            append(RIGHT_EDGE);
        } else {
            append(STRAIGHT_EDGE);
        }
    }

    private void printMergeEdges(int parents) {
        append(MERGE_EDGE);
        pad(RIGHT_EDGE, parents - 2);
    }

    private void cleanMapping() {
        previousBranches.clear();
        previousBranches.addAll(nextBranches);
        nextBranches.clear();
        previousBranches.stream()
                .filter(rev -> rev != null && !nextBranches.contains(rev))
                .forEach(nextBranches::add);
    }

    private void printPostRevisionLines() {
        int height = postRevisionLines();
        if (height <= 0) {
            return;
        }
        PrintGrid grid = new PrintGrid(previousBranches.size() * 2, height);
        printStraightEdges(grid);
        printSkewEdges(grid);

        grid.render().forEach(line -> {
            append(line);
            newLine();
        });
    }

    private int postRevisionLines() {
        int max = 0;
        for (int i = 0; i < previousBranches.size(); i++) {
            Hash rev = previousBranches.get(i);
            if (rev != null) {
                max = Math.max(max, i - nextBranches.indexOf(rev));
            }
        }
        return max * 2 - 1;
    }

    private void printStraightEdges(PrintGrid grid) {
        for (int i = 0; i < previousBranches.size(); i++) {
            Hash rev = previousBranches.get(i);
            if (rev == null || nextBranches.indexOf(rev) != i) {
                continue;
            }
            for (int y = 0; y < grid.getHeight(); y++) {
                grid.print(2 * i, y, STRAIGHT_EDGE);
            }
        }
    }

    private void printSkewEdges(PrintGrid grid) {
        for (int i = 0; i < previousBranches.size(); i++) {
            Hash rev = previousBranches.get(i);
            int nextIdx = nextBranches.indexOf(rev);
            if (rev == null || nextIdx == i) {
                continue;
            }
            int x = 2 * i - 1;
            int y = 0;
            while (y < grid.getHeight() && x > nextIdx) {
                grid.print(x, y, LEFT_EDGE);
                x--;
                y++;
            }
        }
    }

    private void pad(String pattern, int count) {
        for (int i = 0; i < count; i++) {
            append(pattern);
        }
    }

    private void append(String text) {
        builder.append(text);
        cursor += text.length();
    }

    private void newLine() {
        builder.append(System.lineSeparator());
        cursor = 0;
    }
}
