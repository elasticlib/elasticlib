package store.server.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.Map.Entry;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import store.common.ContentInfo;
import store.common.Hash;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;

public class IndexManager {

    private final Directory directory;
    private final Analyzer analyzer;
    private final IndexWriterConfig config;

    private IndexManager(Path root) {
        directory = new TransactionalDirectory(root);
        analyzer = new StandardAnalyzer(Version.LUCENE_44);
        config = new IndexWriterConfig(Version.LUCENE_44, analyzer);
    }

    public static IndexManager create(Path path) {
        try {
            Files.createDirectory(path);
            return new IndexManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static IndexManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        return new IndexManager(path);
    }

    public void add(ContentInfo contentInfo) {
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            Document document = new Document();
            document.add(new TextField("hash", contentInfo.getHash().encode(), Store.YES));
            document.add(new TextField("_all", contentInfo.getHash().encode(), Store.NO));
            document.add(new LongField("length", contentInfo.getLength(), Store.NO));
            for (Entry<String, Object> entry : contentInfo.getMetadata().entrySet()) {
                document.add(new TextField(entry.getKey(), entry.getValue().toString(), Store.NO));
                if (entry.getValue() instanceof String) {
                    document.add(new TextField("_all", entry.getValue().toString(), Store.NO));
                }
            }
            writer.addDocument(document, analyzer);
            writer.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Hash> find(String query) {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            QueryParser parser = new QueryParser(Version.LUCENE_44, "_all", analyzer);
            ScoreDoc[] hits = searcher.search(parser.parse(query), 1000).scoreDocs;

            List<Hash> hashes = new ArrayList<>(hits.length);
            for (ScoreDoc hit : hits) {
                Document document = searcher.doc(hit.doc, singleton("hash"));
                hashes.add(new Hash(document.getValues("hash")[0]));
            }
            return hashes;

        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
