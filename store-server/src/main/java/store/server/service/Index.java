package store.server.service;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import static com.google.common.io.BaseEncoding.base16;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Math.min;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Hash;
import store.common.IndexEntry;
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
 * A Lucene index on a repository.
 */
class Index {

    private static final Logger LOG = LoggerFactory.getLogger(Index.class);
    private final String name;
    private final Directory directory;
    private final Analyzer analyzer;

    private Index(String name, Path path) throws IOException {
        this.name = name;
        directory = FSDirectory.open(path.toFile(), new SingleInstanceLockFactory());
        analyzer = new StandardAnalyzer(Version.LUCENE_46);
    }

    private IndexWriter newIndexWriter() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
        return new IndexWriter(directory, config);
    }

    /**
     * Create a new index.
     *
     * @param name index name.
     * @param path index home.
     * @return Created index.
     */
    public static Index create(String name, Path path) {
        try {
            Files.createDirectory(path);
            return new Index(name, path);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Open an existing index.
     *
     * @param name index name.
     * @param path index home.
     * @return Opened index.
     */
    public static Index open(String name, Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        try {
            return new Index(name, path);

        } catch (IOException ex) {
            throw new WriteException(ex);
        }
    }

    /**
     * Index supplied content.
     *
     * @param contentInfo Info associated with content to index.
     * @param inputStream Input stream of the content to index.
     */
    public void put(ContentInfoTree contentInfoTree, InputStream inputStream) {
        LOG.info("[{}] Indexing {}", name, contentInfoTree.getHash());
        Optional<IndexEntry> existing = getEntry(contentInfoTree.getHash());
        if (existing.isPresent() && existing.get().getHead().equals(contentInfoTree.getHead())) {
            // already indexed !
            return;
        }
        try (IndexWriter writer = newIndexWriter()) {
            // First delete any existing document.
            writer.deleteDocuments(new Term("hash", contentInfoTree.getHash().encode()));

            // Then (re)create the document.
            Document document = new Document();
            document.add(new TextField("hash", contentInfoTree.getHash().encode(), Store.YES));
            for (Hash rev : contentInfoTree.getHead()) {
                document.add(new TextField("rev", rev.encode(), Store.YES));
            }
            document.add(new LongField("length", contentInfoTree.getLength(), Store.NO));
            for (Entry<String, Collection<Value>> entry : headMetadata(contentInfoTree).asMap().entrySet()) {
                String key = entry.getKey();
                for (Value value : entry.getValue()) {
                    add(document, key, value);
                }
            }
            document.add(new TextField("content", new Tika().parse(inputStream)));
            writer.addDocument(document, analyzer);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private Multimap<String, Value> headMetadata(ContentInfoTree contentInfoTree) {
        Multimap<String, Value> metadata = HashMultimap.create();
        for (Hash rev : contentInfoTree.getHead()) {
            ContentInfo contentInfo = contentInfoTree.get(rev);
            for (Entry<String, Value> entry : contentInfo.getMetadata().entrySet()) {
                metadata.put(entry.getKey(), entry.getValue());
            }
        }
        return metadata;
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

    private Optional<IndexEntry> getEntry(Hash hash) {
        try {
            if (directory.listAll().length == 0) {
                return Optional.absent();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TermQuery query = new TermQuery(new Term("hash", hash.encode()));
                ScoreDoc[] hits = searcher.search(query, 1).scoreDocs;
                if (hits.length == 0) {
                    return Optional.absent();
                }
                return Optional.of(newIndexEntry(searcher.doc(hits[0].doc)));
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Delete all index entry about content which hash is supplied.
     *
     * @param hash Hash of this content.
     */
    public void delete(Hash hash) {
        LOG.info("[{}] Deleting {}", name, hash);
        try (IndexWriter writer = newIndexWriter()) {
            writer.deleteDocuments(new Term("hash", hash.encode()));

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Find index entries matching supplied query.
     *
     * @param query Search query.
     * @param first First result to return.
     * @param number Number of results to return.
     * @return A list of content hashes.
     */
    public List<IndexEntry> find(String query, int first, int number) {
        LOG.info("[{}] Finding {}, first {}, count {}", name, query, first, number);
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
                QueryParser parser = new QueryParser(Version.LUCENE_46, "content", analyzer);
                ScoreDoc[] hits = searcher.search(parser.parse(query), first + number).scoreDocs;
                List<IndexEntry> entries = new ArrayList<>(number);
                int last = min(first + number, hits.length);
                for (int i = first; i < last; i++) {
                    Document document = searcher.doc(hits[i].doc);
                    Hash hash = new Hash(document.getValues("hash")[0]);
                    Set<Hash> head = new HashSet<>();
                    for (String value : document.getValues("rev")) {
                        head.add(new Hash(value));
                    }
                    entries.add(new IndexEntry(hash, head));
                }
                return entries;
            }
        } catch (ParseException e) {
            throw new BadRequestException(e);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private IndexEntry newIndexEntry(Document document) {
        Hash hash = new Hash(document.getValues("hash")[0]);
        Set<Hash> head = new HashSet<>();
        for (String value : document.getValues("rev")) {
            head.add(new Hash(value));
        }
        return new IndexEntry(hash, head);
    }
}
