package store.client.util;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import jline.console.ConsoleReader;
import jline.console.CursorBuffer;
import static jline.console.completer.CandidateListCompletionHandler.printCandidates;
import static jline.console.completer.CandidateListCompletionHandler.setBuffer;
import jline.console.completer.CompletionHandler;
import store.client.tokenizing.Tokenizing;

/**
 * A completion handler aware of argument escaping.
 */
public class EscapingCompletionHandler implements CompletionHandler {

    /**
     * Handle completion.
     *
     * @param reader Current console reader.
     * @param candidates Completion candidates.
     * @param pos Completion position.
     * @return true if completion has succeeded.
     * @throws IOException If an IO error happens.
     */
    @Override
    public boolean complete(ConsoleReader reader, List<CharSequence> candidates, int pos) throws IOException {
        CursorBuffer buf = reader.getCursorBuffer();
        if (candidates.size() == 1) {
            CharSequence value = candidates.get(0);
            if (value.equals(buf.toString())) {
                return false;
            }
            setBuffer(reader, Tokenizing.escape(value.toString()) + " ", pos);
            return true;

        } else if (candidates.size() > 1) {
            String value = getUnambiguousCompletions(candidates);
            setBuffer(reader, value, pos);
        }
        if (new HashSet<>(candidates).size() > reader.getAutoprintThreshold()) {
            reader.println();
        }
        printCandidates(reader, candidates);
        reader.drawLine();
        return true;
    }

    private String getUnambiguousCompletions(List<CharSequence> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        List<String> strings = Lists.transform(candidates, new Function<CharSequence, String>() {
            @Override
            public String apply(CharSequence input) {
                return Tokenizing.escape(input.toString());
            }
        });
        String first = strings.get(0);
        StringBuilder candidate = new StringBuilder();
        int i = 0;
        while (i < first.length() && startsWith(first.substring(0, i + 1), strings)) {
            candidate.append(first.charAt(i));
            i++;
        }
        return candidate.toString();
    }

    private static boolean startsWith(String starts, List<String> candidates) {
        for (String candidate : candidates) {
            if (!candidate.startsWith(starts)) {
                return false;
            }
        }
        return true;
    }
}
