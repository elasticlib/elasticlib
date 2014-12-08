package org.elasticlib.console.display;

/**
 * CSI color codes.
 */
enum Color {

    BOLD_GREEN("32;1"),
    BOLD_BLUE("34;1"),
    RESET("0");
    private final String code;

    private Color(String code) {
        this.code = "\u001b[" + code + "m";
    }

    @Override
    public String toString() {
        return code;
    }
}
