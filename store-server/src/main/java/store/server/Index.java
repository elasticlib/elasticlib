package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Version;
import org.apache.tika.Tika;
import store.common.ContentInfo;
import store.common.Hash;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;

public class Index {

    private final Directory directory;
    private final Analyzer analyzer;
    private final IndexWriterConfig config;

    public Index(Path path) throws IOException {
        directory = FSDirectory.open(path.toFile(), new SingleInstanceLockFactory());
        analyzer = new StandardAnalyzer(Version.LUCENE_44);
        config = new IndexWriterConfig(Version.LUCENE_44, analyzer);
    }

    public static Index create(Path path) {
        try {
            Files.createDirectory(path);
            return new Index(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static Index open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        try {
            return new Index(path);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void put(ContentInfo contentInfo, InputStream inputStream) {
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            Document document = new Document();
            document.add(new TextField("hash", contentInfo.getHash().encode(), Store.YES));
            document.add(new LongField("length", contentInfo.getLength(), Store.NO));
            for (Entry<String, Object> entry : contentInfo.getMetadata().entrySet()) {
                document.add(new TextField(entry.getKey(), entry.getValue().toString(), Store.NO));
            }
            document.add(new TextField("content", new Tika().parse(inputStream)));
            writer.addDocument(document, analyzer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(Hash hash) {
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            writer.deleteDocuments(new Term("hash", hash.encode()));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean contains(Hash hash) {
        try {
            if (directory.listAll().length == 0) {
                return false;
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TermQuery query = new TermQuery(new Term("hash", hash.encode()));
                return searcher.search(query, 1).totalHits > 0;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Hash> find(String query) {
        try {
            if (directory.listAll().length == 0) {
                return emptyList();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                QueryParser parser = new QueryParser(Version.LUCENE_44, "content", analyzer);
                ScoreDoc[] hits = searcher.search(parser.parse(query), 1000).scoreDocs;

                List<Hash> hashes = new ArrayList<>(hits.length);
                for (ScoreDoc hit : hits) {
                    Document document = searcher.doc(hit.doc, singleton("hash"));
                    hashes.add(new Hash(document.getValues("hash")[0]));
                }
                return hashes;
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
