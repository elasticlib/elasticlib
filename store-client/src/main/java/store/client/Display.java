package store.client;

import java.io.PrintWriter;
import jline.console.ConsoleReader;

public class Display {

    private static final String PROMPT = "> ";
    private final ConsoleReader consoleReader;
    private final PrintWriter out;

    public Display(ConsoleReader consoleReader) {
        this.consoleReader = consoleReader;
        out = new PrintWriter(consoleReader.getOutput());
        consoleReader.setPrompt(PROMPT);
    }

    public void print(String line) {
        out.println(line);
        out.flush();
    }

    public void setPrompt(String prompt) {
        consoleReader.setPrompt(prompt + PROMPT);
    }

    public void resetPrompt() {
        consoleReader.setPrompt(PROMPT);
    }
}
