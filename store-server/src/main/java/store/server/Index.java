package store.server;

import static com.google.common.io.BaseEncoding.base16;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Math.min;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.Map.Entry;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools.Resolution;
import static org.apache.lucene.document.DateTools.dateToString;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
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
import store.common.value.Value;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.BYTE;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.INT;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;
import store.server.exception.BadRequestException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.WriteException;

/**
 * A Lucene index on a volume.
 */
public class Index {

    private final Directory directory;
    private final Analyzer analyzer;

    private Index(Path path) throws IOException {
        directory = FSDirectory.open(path.toFile(), new SingleInstanceLockFactory());
        analyzer = new StandardAnalyzer(Version.LUCENE_46);
    }

    private IndexWriter newIndexWriter() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
        return new IndexWriter(directory, config);
    }

    public static Index create(Path path) {
        try {
            Files.createDirectory(path);
            return new Index(path);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    public static Index open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        try {
            return new Index(path);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void put(ContentInfo contentInfo, InputStream inputStream) {
        try (IndexWriter writer = newIndexWriter()) {
            Document document = new Document();
            document.add(new TextField("hash", contentInfo.getHash().encode(), Store.YES));
            document.add(new LongField("length", contentInfo.getLength(), Store.NO));
            for (Entry<String, Value> entry : contentInfo.getMetadata().entrySet()) {
                String key = entry.getKey();
                Value value = entry.getValue();
                add(document, key, value);
            }
            document.add(new TextField("content", new Tika().parse(inputStream)));
            writer.addDocument(document, analyzer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void add(Document document, String key, Value value) {
        switch (value.type()) {
            case BOOLEAN:
                document.add(new TextField(key, value.asBoolean() ? "true" : "false", Store.NO));
                return;

            case BYTE:
            case INT:
                document.add(new IntField(key, value.asInt(), Store.NO));
                return;

            case LONG:
                document.add(new LongField(key, value.asLong(), Store.NO));
                return;

            case BIG_DECIMAL:
                document.add(new DoubleField(key, value.asBigDecimal().doubleValue(), Store.NO));
                return;

            case STRING:
                document.add(new TextField(key, value.asString(), Store.NO));
                return;

            case DATE:
                document.add(new TextField(key, dateToString(value.asDate(), Resolution.SECOND), Store.NO));
                return;

            case BINARY:
                document.add(new TextField(key, base16().lowerCase().encode(value.asByteArray()), Store.NO));
                return;

            case ARRAY:
                for (Value item : value.asList()) {
                    add(document, key, item);
                }
                return;

            case OBJECT:
                for (Entry<String, Value> entry : value.asMap().entrySet()) {
                    add(document, key + "." + entry.getKey(), entry.getValue());
                }
                return;

            case NULL:
                return;

            default:
                throw new IllegalArgumentException(value.type().toString());
        }
    }

    public void delete(Hash hash) {
        try (IndexWriter writer = newIndexWriter()) {
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

    public List<Hash> find(String query, int first, int number) {
        try {
            if (first < 0) {
                number += first;
                first = 0;
            }
            if (directory.listAll().length == 0 || number <= 0) {
                return emptyList();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                QueryParser parser = new QueryParser(Version.LUCENE_44, "content", analyzer);
                ScoreDoc[] hits = searcher.search(parser.parse(query), first + number).scoreDocs;
                List<Hash> hashes = new ArrayList<>(number);
                int last = min(first + number, hits.length);
                for (int i = first; i < last; i++) {
                    Document document = searcher.doc(hits[i].doc, singleton("hash"));
                    hashes.add(new Hash(document.getValues("hash")[0]));
                }
                return hashes;
            }
        } catch (ParseException e) {
            throw new BadRequestException(e);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
