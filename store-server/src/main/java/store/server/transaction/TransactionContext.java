package store.server.transaction;

import java.nio.file.Path;

public interface TransactionContext {
    //
    //   \o/   \o/   \o/   \o/   \o/   \o/   \o/   \o/
    //    |     |     |     |     |     |     |     |
    //   / \   / \   / \   / \   / \   / \   / \   / \
    //
    // TODO ?
    //
    // Simplifier l'API :
    //
    // - Erreur si ouverture sur un fichier qui n'existe pas (EmptyInput disparait)
    //
    // - Ajout méthodes :
    //      truncate(Path, length)
    //      exists(Path)
    //      create(Path)
    //
    // - Output toujours en append.
    // - Surcharge où l'on fourni à l'avance la longueur à écrire (hint pour heavyWrite)
    //

    Input openInput(Path path);

    Output openTruncatingOutput(Path path);

    Output openAppendingOutput(Path path);

    Output openHeavyWriteOutput(Path path);

    void delete(Path path);
}
