/**
 * 
 */
package code.dws.indexer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Creates a Lucene indexes on a file, mostly the input OIE data file
 * 
 * @author arnab
 */
public class DataIndexer
{

    public static Logger logger = Logger.getLogger(DataIndexer.class.getName());

    private static String INPUT_OIE_FILE = null;

    private static final String DATA_DELIMIT = ";";

    private static String INDEX_DIR = null;

    private static boolean create;

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        if (args.length != 1)
            throw new IllegalArgumentException();
        else
            INPUT_OIE_FILE = args[0];

        if (INPUT_OIE_FILE != null)
            INDEX_DIR = new File(INPUT_OIE_FILE).getParent() + "/index/";

        create = true;
        indexer();
    }

    /**
     * Creates index over the DBPedia data located in the directory mentioned in the {@link Constants}
     * 
     * @throws Exception
     */
    public static void indexer() throws Exception
    {

        IndexWriter writer = null;
        Analyzer analyzer = null;

        final File docDir = new File(INPUT_OIE_FILE);
        if (!docDir.exists() || !docDir.canRead()) {
            System.out.println("Document directory '" + docDir.getAbsolutePath()
                + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        logger.info("Started indexing file at " + docDir);

        // create a directory of the Indices
        Directory indexDirectory = FSDirectory.open(new File(INDEX_DIR));

        analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_0, analyzer);

        if (create) {
            // Create a new index in the directory, removing any
            // previously indexed documents:
            iwc.setOpenMode(OpenMode.CREATE);
        } else {
            // Add new documents to an existing index:
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        }

        // iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        // create a Index writer
        writer = new IndexWriter(indexDirectory, iwc);

        // start timer
        long start = Utilities.startTimer();

        // start indexing iteratively all files at the location
        indexDocs(writer, docDir);

        writer.forceMerge(1);
        writer.commit();
        writer.close();

        // end timer
        Utilities.endTimer(start, "INDEXING COMPLETED IN ");
    }

   
    private static void indexDocs(IndexWriter writer, File file) throws IOException
    {
        // do not try to index files that cannot be read
        if (file.canRead()) {
            if (file.isDirectory()) {
                String[] files = file.list();
                // an IO error could occur
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        indexDocs(writer, new File(file, files[i]));
                    }
                }
            } else {

                logger.info("Indexing " + file.getAbsolutePath());
                String strLine = null;
                FileInputStream fstream = null;

                /**
                 * Lucene indexes on particular fields. We create two fields for the URI and one for the labels
                 */

                Field oieSubField = null;
                Field oieRelField = null;
                Field oieObjField = null;

                String oieSub = null;
                String oieRel = null;
                String oieObj = null;

                // int isFrequent = 0;

                try {
                    fstream = new FileInputStream(file);
                    DataInputStream in = new DataInputStream(fstream);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));

                    // create document object
                    Document document = null;
                    String[] array = null;

                    // read comma separated file line by line
                    while ((strLine = br.readLine()) != null) {

                        // break comma separated line using ","
                        array = strLine.split(DATA_DELIMIT);

                        if (array.length == 4) {
                            oieSub = array[0];
                            oieRel = array[1];
                            oieObj = array[2];

                            // define all the fields to be indexed
                            oieSubField = new StringField("oieSubField", oieSub.trim().toLowerCase(), Field.Store.YES);
                            oieRelField = new StringField("oieRelField", oieRel.trim().toLowerCase(), Field.Store.YES);
                            oieObjField = new StringField("oieObjField", oieObj.trim().toLowerCase(), Field.Store.YES);

//                            System.out.println(oieSub + "\t" + oieRel + "\t" + oieObj);
                            // add to document
                            document = new Document();
                            document.add(oieSubField);
                            document.add(oieRelField);
                            document.add(oieObjField);

                            // add the document finally into the writer
                            writer.addDocument(document);

                        }
                    }

                } catch (Exception ex) {
                    logger.error(ex.getMessage() + " while reading  " + strLine);

                } finally {
                    // Close the input stream
                    fstream.close();
                }
            }
        }
    }

}
