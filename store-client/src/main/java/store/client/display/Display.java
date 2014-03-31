package store.client.display;

import java.io.PrintWriter;
import jline.console.ConsoleReader;
import static store.client.display.FormatUtil.asString;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;

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
        out.print(line);
        out.flush();
    }

    public void println(String line) {
        out.println(line);
        out.flush();
    }

    public void print(ContentInfoTree tree) {
        println(asString(tree));
    }

    public void print(ContentInfo info) {
        println(asString(info));
    }

    public void print(Event event) {
        println(asString(event));
    }

    public void print(CommandResult result) {
        println(asString(result));
    }

    public void setPrompt(String prompt) {
        consoleReader.setPrompt(prompt + PROMPT);
    }

    public void resetPrompt() {
        consoleReader.setPrompt(PROMPT);
    }
}
