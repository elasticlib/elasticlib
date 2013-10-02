package store.server.index;

import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import store.server.transaction.TransactionContext;
import static store.server.transaction.TransactionManager.currentTransactionContext;

class TransactionalDirectory extends Directory {

    private final Path root;

    public TransactionalDirectory(Path root) {
        this.root = root;
        lockFactory = NoLockFactory.getNoLockFactory();
    }

    @Override
    public String[] listAll() {
        return currentTransactionContext().listFiles(root);
    }

    @Override
    public boolean fileExists(String name) {
        return currentTransactionContext().exists(root.resolve(name));
    }

    @Override
    public long fileLength(String name) {
        return currentTransactionContext().fileLength(root.resolve(name));
    }

    @Override
    public void deleteFile(String name) {
        currentTransactionContext().delete(root.resolve(name));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        Path path = root.resolve(name);
        TransactionContext txContext = currentTransactionContext();
        if (!txContext.exists(path)) {
            txContext.create(path);
        }
        return new TransactionalIndexOutput(currentTransactionContext().openOutput(path));
    }

    @Override
    public IndexInput openInput(String name, IOContext context) {
        Path path = root.resolve(name);
        TransactionContext txContext = currentTransactionContext();
        if (!txContext.exists(path)) {
            return new EmptyInput(name);
        }
        return new TransactionalIndexInput(name, txContext.fileLength(path), txContext.openInput(path));
    }

    @Override
    public void sync(Collection<String> names) {
        // RAF commit non géré par lucene.
    }

    @Override
    public void close() {
        // RAF
    }

    @Override
    public IndexInputSlicer createSlicer(final String name, final IOContext context) throws IOException {
        return new IndexInputSlicer() {
            private final Path path = root.resolve(name);
            private final IndexInput input = openInput(name, context);
            //
            // Cette implé fonctionne, mais danger :
            //
            // - Niveau perf, on charge la slice entière en mémoire. Pas cool...
            // - Dans l'absolu, même si la ram encaisse, un byte[] est limité en taille à 2 giga-octets !
            //
            // Remplacer par un slicer qui ouvre autant d'inputs que nécessaire.
            // A la fermeture, il les ferme tous.
            // A noter :
            // - chaque slice est ouvert sur le length demandé.
            // - le fichier physique est seeké sur l'offet demandé à l'ouverture du slice.
            // - le seek() et getFilePointer() ultérieurs seront relatifs à cet offset.
            //
            // Sinon, à la limite fournir des slice pure mémoire si length < BUFFER_SIZE (par exemple 8192)
            //

            @Override
            public IndexInput openSlice(String slice, long offset, long length) throws IOException {
                input.seek(offset);
                int len = Ints.checkedCast(length);
                byte[] data = new byte[len];
                input.readBytes(data, 0, len);
                return new SliceInput(desc(name, slice), data);
            }

            private String desc(String name, String slice) {
                return new StringBuilder()
                        .append(name)
                        .append(" [slice=")
                        .append(slice)
                        .append("]")
                        .toString();
            }

            @Override
            public void close() throws IOException {
                input.close();
            }

            @Override
            public IndexInput openFullSlice() throws IOException {
                return openSlice("fullSlice", 0, currentTransactionContext().fileLength(path));
            }
        };
    }

    private final static class SliceInput extends IndexInput {

        private byte[] data;
        private int pos;

        SliceInput(String desc, byte[] data) throws IOException {
            super(desc);
            this.data = data;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public long getFilePointer() {
            return pos;
        }

        @Override
        public void seek(long pos) {
            this.pos = (int) (pos);
        }

        @Override
        public byte readByte() {
            return data[pos++];
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) {
            System.arraycopy(data, pos, b, offset, len);
            pos += len;
        }

        @Override
        public void close() {
            data = null;
        }
    }
}
