package code.dws.indexer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.FSDirectory;

import code.dws.utils.Utilities;

public class DataSearcher {

	private static String INDEX_DIR = null;

	private static final int MAX_RESULTS = 2000;

	public static Logger logger = Logger
			.getLogger(DataSearcher.class.getName());

	private static String INPUT_OIE_FILE;

	public static void main(String[] args) throws IOException {
		if (args.length != 1)
			throw new IllegalArgumentException();
		else
			INPUT_OIE_FILE = args[0];

		INDEX_DIR = new File(INPUT_OIE_FILE).getParent() + "/index/";

		// uncomment to use as a test client
		doSearch("eight", "ch");
		logger.info("Lucene Index ready for query");
	}

	/**
	 * @param userQuery
	 *            user input term
	 * @param field1
	 *            first argument denoting the index field to match to
	 * @param field2
	 *            second argument denoting the index field to match
	 * @param identifier
	 * @return a Boolean query where both the indices should be matched
	 */
	public static BooleanQuery frameQuery(String userQuery, String userQuery2,
			String field1, String field2) {
		BooleanQuery subQuery = new BooleanQuery();
		BooleanQuery subQuery00 = new BooleanQuery();
		BooleanQuery subQuery01 = new BooleanQuery();
		BooleanQuery subQuery10 = new BooleanQuery();
		BooleanQuery subQuery11 = new BooleanQuery();

		subQuery00.add(
				new TermQuery(new Term(field1, userQuery.toLowerCase())),
				BooleanClause.Occur.MUST);
		subQuery00.add(
				new TermQuery(new Term(field2, userQuery2.toLowerCase())),
				BooleanClause.Occur.MUST);

		subQuery01.add(
				new TermQuery(new Term(field1, userQuery.toLowerCase())),
				BooleanClause.Occur.MUST);
		subQuery01.add(
				new WildcardQuery(new Term(field2, userQuery2.toLowerCase()
						+ "*")), BooleanClause.Occur.MUST);

		subQuery10.add(
				new WildcardQuery(new Term(field1, userQuery.toLowerCase()
						+ "*")), BooleanClause.Occur.MUST);
		subQuery10.add(
				new TermQuery(new Term(field2, userQuery2.toLowerCase())),
				BooleanClause.Occur.MUST);

		subQuery11.add(
				new WildcardQuery(new Term(field1, userQuery.toLowerCase()
						+ "*")), BooleanClause.Occur.MUST);
		subQuery11.add(
				new WildcardQuery(new Term(field2, userQuery2.toLowerCase()
						+ "*")), BooleanClause.Occur.MUST);

		subQuery.add(subQuery00, BooleanClause.Occur.SHOULD);
		subQuery.add(subQuery01, BooleanClause.Occur.SHOULD);
		subQuery.add(subQuery10, BooleanClause.Occur.SHOULD);
		subQuery.add(subQuery11, BooleanClause.Occur.SHOULD);

		return subQuery;
	}

	/**
	 * method accepts a user query and fetches over the indexed DBPedia data
	 * 
	 * @param subQuery
	 *            the user provided search item
	 * @param file
	 * @param field
	 * @return
	 * @return A List containing the matching DBPedia Entity URI as value
	 * @throws Exception
	 */
	public static List<String> doSearch(String subjQuery, String objQuery)
			throws IOException {
		Query subQuery = null;
		IndexReader reader = null;
		IndexSearcher searcher = null;

		long start = 0;

		TopDocs hits = null;

		try {

			// start timer
			start = Utilities.startTimer();

			// create index reader object
			// reader = IndexReader.open(FSDirectory.open(file));
			reader = DirectoryReader
					.open(FSDirectory.open(new File(INDEX_DIR)));

			// create index searcher object
			searcher = new IndexSearcher(reader);

			// remove any un-necessary punctuation marks from the query
			subjQuery = subjQuery.toLowerCase().trim();
			objQuery = objQuery.toLowerCase().trim();

			logger.info("Searching for: " + subjQuery + ", " + objQuery);

			// frame a query on the surname field
			subQuery = frameQuery(subjQuery, objQuery, "oieSubField",
					"oieObjField");
			// subQuery = new TermQuery(new Term("oieSubField",
			// userQuery.toLowerCase()));

			// execute the search on top results
			hits = searcher.search(subQuery, null, MAX_RESULTS);
			if (hits.totalHits > 0)
				return iterateResult(searcher, hits);

			// still we have no result then perform wild card search

		} catch (Exception ex) {
			logger.info("NO MATCHING RECORDS FOUND FOR QUERY \"" + subQuery
					+ "\" !! ");
		} finally {

			// Utilities.endTimer(start, "QUERY \"" + subQuery +
			// "\" ANSWERED IN ");
		}
		return null;
	}

	/**
	 * @param searcher
	 *            searcher instance
	 * @param setURI
	 *            a set to identify the unique URI s
	 * @param resultMap
	 *            a result map sorted by best matches
	 * @param hits
	 *            document hits instance
	 * @param userQuery
	 *            the user input coming from web interface or extraction engines
	 * @return
	 * @throws IOException
	 */
	public static List<String> iterateResult(IndexSearcher searcher,
			TopDocs hits) throws IOException {
		String oieSub;
		String oieRel;
		String oieObj;

		List<String> retProps = new ArrayList<String>();

		// iterate over the results fetched after index search
		for (ScoreDoc scoredoc : hits.scoreDocs) {
			// Retrieve the matched document and show relevant details
			Document doc = searcher.doc(scoredoc.doc);

			oieSub = doc.get("oieSubField");
			oieRel = doc.get("oieRelField");
			oieObj = doc.get("oieObjField");

			logger.info(oieSub + "\t " + oieRel + "\t" + oieObj);
			retProps.add(oieRel);
		}

		return retProps;
	}
}
