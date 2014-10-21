/**
 * 
 */
package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * This is another way of finding new triples, Here, we do not try to find what
 * DBPedia proeprty a cluster might map to, but feed the set of DBpedia property
 * along with Reverb and cluster all together.
 * 
 * 
 * @author adutta
 * 
 */
public class ClusteringWithDbpedia {

	private static final String QUERY = "select distinct ?val where {?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>} ";

	/**
	 * logger
	 */
	// define Logger
	public static Logger logger = Logger.getLogger(ClusteringWithDbpedia.class
			.getName());

	static BufferedWriter writerDbpProps = null;

	static int k = -1; // ReverbClusterProperty.TOPK_REV_PROPS;

	/**
	 * initialize writers.
	 */
	private static void init() {
		try {

			writerDbpProps = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent()
					+ "/dbp."
					+ k
					+ ".object.properties.sim.csv"));

		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	/**
	 * 
	 */
	public ClusteringWithDbpedia() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		int cnt = 0;

		Constants.loadConfigParameters(new String[] { "", args[0] });

		init();

		List<String> dbpProps = null;

		String arg1 = null;
		String arg2 = null;

		double simScore = 0;
		// call to retrieve DBPedia owl object property
		dbpProps = loadDbpediaProperties(k);

		logger.info("Loaded " + dbpProps.size() + " DBpedia properties");

		logger.info("Writing sim scores to "
				+ new File(Constants.OIE_DATA_PATH).getParent()
				+ "/all.trvb.dbp." + k + ".pairwise.sim.csv");

		long start = Utilities.startTimer();
		long propSize = dbpProps.size() * (dbpProps.size() - 1);

		ExecutorService executorPool = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());

		try {
			for (int outer = 0; outer < 6; outer++) {
				for (int inner = outer + 1; inner < 6; inner++) {

					cnt++;

					arg1 = Utilities.splitAtCapitals(dbpProps.get(outer));
					arg2 = Utilities.splitAtCapitals(dbpProps.get(inner));

					try {
						Future<Double> future = executorPool.submit(new Worker(
								arg1, arg2));

						if (future.get() != null) {
							simScore = future.get();
							writerDbpProps.write(arg1 + "\t" + arg2 + "\t"
									+ simScore + "\n");
							writerDbpProps.flush();
						}
					} catch (ExecutionException e) {
						logger.error(e.getMessage());
					}

					if (cnt > 1000 && cnt % 1000 == 0)
						Utilities.endTimer(start, 200
								* ((double) cnt / propSize)
								+ " percent done in ");

				}
			}

			executorPool.shutdown();
			while (!executorPool.isTerminated()) {
			}

		} catch (Exception e) {
			logger.error(e.getMessage());

		} finally {
			writerDbpProps.close();
		}

	}

	/**
	 * load DBP properties from SPARQL endpoint, -1 means all properties
	 * 
	 * @param topKDBPediaProperties
	 * 
	 * @return
	 */
	public static List<String> loadDbpediaProperties(long topKDBPediaProperties) {

		String prop = null;
		String cnt = "0";
		int c = 0;

		List<String> retS = new ArrayList<String>();

		Map<String, Long> props = new HashMap<String, Long>();

		List<QuerySolution> count = null;

		List<QuerySolution> dbpObjProps = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(QUERY);

		for (QuerySolution querySol : dbpObjProps) {
			prop = querySol.get("val").toString();

			if ((prop.indexOf(Constants.DBPEDIA_PREDICATE_NS) != -1)
					&& (prop.indexOf("wikiPageWikiLink") == -1)
					&& (prop.indexOf("wikiPageExternalLink") == -1)
					&& (prop.indexOf("wikiPageRedirects") == -1)
					&& (prop.indexOf("thumbnail") == -1)
					&& (prop.indexOf("wikiPageDisambiguates") == -1)
					&& (prop.indexOf("wikiPageInterLanguageLink") == -1)) {

				count = SPARQLEndPointQueryAPI
						.queryDBPediaEndPoint("select (count(*)  as ?val)  where {?a <"
								+ prop + "> ?c} ");

				for (QuerySolution sol : count) {
					cnt = sol.get("val").toString();
				}
				cnt = cnt.substring(0, cnt.indexOf("^"));
				props.put(prop.replaceAll(Constants.DBPEDIA_PREDICATE_NS, ""),
						Long.parseLong(cnt));
			}
		}

		// sort only when interested in top-k, else makes no sense
		if (topKDBPediaProperties != -1)
			props = Utilities.sortByValue(props);

		for (Entry<String, Long> e : props.entrySet()) {
			retS.add(e.getKey());

			c++;
			if (c == topKDBPediaProperties)
				return retS;
		}

		return retS;
	}
}
