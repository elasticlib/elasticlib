package store.client.display;

import java.io.PrintWriter;
import jline.console.ConsoleReader;
import static store.client.display.MappableFormatting.format;
import store.common.Mappable;
import static store.common.yaml.YamlWriting.write;

/**
 * Console display interface.
 */
public class Display {

    private static final String PROMPT = "> ";
    private final ConsoleReader consoleReader;
    private final PrintWriter out;

    /**
     * Constructor.
     *
     * @param consoleReader Console.
     */
    public Display(ConsoleReader consoleReader) {
        this.consoleReader = consoleReader;
        out = new PrintWriter(consoleReader.getOutput());
        consoleReader.setPrompt(PROMPT);
    }

    /**
     * Print supplied text.
     *
     * @param text Text to print.
     */
    public void print(String text) {
        out.print(text);
        out.flush();
    }

    /**
     * Print supplied text and append a line-return.
     *
     * @param text Text to print.
     */
    public void println(String text) {
        out.println(text);
        out.flush();
    }

    /**
     * Print supplied mappable and append a line-return.
     *
     * @param mappable Mappable to print.
     */
    public void print(Mappable mappable) {
        println(write(format(mappable)));
    }

    /**
     * Set prompt to display.
     *
     * @param prompt Prompt to display (excluding actual prompt-separator).
     */
    public void setPrompt(String prompt) {
        consoleReader.setPrompt(prompt + PROMPT);
    }

    /**
     * Clear displayed prompt (excluding separator).
     */
    public void resetPrompt() {
        consoleReader.setPrompt(PROMPT);
    }
}
