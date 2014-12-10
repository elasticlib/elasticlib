/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.console.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import jline.console.ConsoleReader;
import jline.console.CursorBuffer;
import static jline.console.completer.CandidateListCompletionHandler.printCandidates;
import static jline.console.completer.CandidateListCompletionHandler.setBuffer;
import jline.console.completer.CompletionHandler;
import org.elasticlib.console.tokenizing.Tokenizing;

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
        List<String> strings = candidates.stream()
                .map(x -> Tokenizing.escape(x.toString()))
                .collect(Collectors.toList());

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
        return candidates.stream().allMatch(x -> x.startsWith(starts));
    }
}
