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
package org.elasticlib.console.tokenizing;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a command line buffer into a list of lexemes.
 */
class Lexer {

    private final List<Lexeme> lexemes = new ArrayList<>();
    private final StringBuilder builder = new StringBuilder();
    private final String buffer;
    private int index;

    public Lexer(String buffer) {
        this.buffer = buffer;
    }

    public List<Lexeme> lex() {
        while (index < buffer.length()) {
            char current = buffer.charAt(index);
            index++;
            switch (current) {
                case Chars.EXCLAMATION_MARK:
                    add(new Lexeme(LexemeType.EXCLAMATION_MARK, current));
                    break;

                case Chars.QUOTE:
                    add(new Lexeme(LexemeType.QUOTE, current));
                    break;

                case Chars.SPACE:
                case Chars.LF:
                case Chars.CR:
                    add(new Lexeme(LexemeType.BLANK, current));
                    break;

                default:
                    builder.append(current);
                    break;
            }
        }
        flush();
        return lexemes;
    }

    private void add(Lexeme lexeme) {
        flush();
        lexemes.add(lexeme);
    }

    private void flush() {
        if (builder.length() > 0) {
            lexemes.add(new Lexeme(LexemeType.TEXT, builder.toString()));
            builder.delete(0, builder.length());
        }
    }
}
