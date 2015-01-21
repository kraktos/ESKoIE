/**
 * 
 */
package code.dws.experiment.goldstandard;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.core.cluster.PairDto;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Distant supervision based
 * 
 * @author adutta
 */
public class KBSeeder {

	// define Logger
	public static Logger logger = Logger.getLogger(KBSeeder.class.getName());

	public static final String SEED_KB = "SEED_KB_FACTS.tsv";

	/**
	 * 
	 */
	public KBSeeder() {

	}

	public static void main(String[] args) throws IOException {
		int k = 0;

		Constants.loadConfigParameters(new String[] { "", args[0] });

		List<String> dbpProps = null;

		// number of KB properties interested in, -1 is all
		long topKKBProperties = -1;

		// get the KB object properties
		// feed based
		dbpProps = SPARQLEndPointQueryAPI.loadDbpediaProperties(
				topKKBProperties, Constants.QUERY_OBJECTTYPE);

		// init DB
		DBWrapper.init(Constants.GET_SF);

		while (true) {
			getKBSampleFeeds(dbpProps);
		}

		// get the ReVerb facts

		// get raw triples

		// scan and match
	}

	/**
	 * @param dbpProps
	 * @param writer
	 * @param randomizer
	 * @param offsetGen
	 * @param completionService
	 * @param taskList
	 */
	public static void getKBSampleFeeds(List<String> dbpProps) {
		BufferedWriter writer = null;
		Random randomizer = new Random();
		Random offsetGen = new Random();

		int cores = Runtime.getRuntime().availableProcessors();
		cores = (cores > Constants.THREAD_MAX_POOL_SIZE) ? cores
				: Constants.THREAD_MAX_POOL_SIZE;

		ExecutorService executorPool = Executors.newFixedThreadPool(cores);
		ExecutorCompletionService<List<PairDto>> completionService = new ExecutorCompletionService<List<PairDto>>(
				executorPool);

		// init task list
		List<Future<List<PairDto>>> taskList = new ArrayList<Future<List<PairDto>>>();

		// init DB
		// DBWrapper.init(Constants.GET_SF);

		long i = 0;
		// iterate the KB properties and find a prop instance prop(sub, obj),
		// randomly
		// repeat for a long time

		while (i++ < 10) {
			final String randomKBProp = dbpProps.get(randomizer
					.nextInt(dbpProps.size()));
			final int randomNum = offsetGen.nextInt(45000) + 1;

			// add to the pool of tasks
			taskList.add(completionService
					.submit(new Callable<List<PairDto>>() {
						@Override
						// find a random instance with this random KB property
						public List<PairDto> call() throws Exception {
							return SPARQLEndPointQueryAPI.getRandomInstance(
									randomKBProp, randomNum);
						}
					}));
		}

		// shutdown pool thread
		executorPool.shutdown();
		try {
			writer = new BufferedWriter(new FileWriter(new File(SEED_KB)));

			long start = System.currentTimeMillis();
			while (!executorPool.isTerminated()) {
				Future<List<PairDto>> futureTask;

				futureTask = completionService.poll(Constants.TIMEOUT_MINS,
						TimeUnit.MINUTES);

				if (futureTask != null) {
					List<PairDto> result = futureTask.get();
					if (result != null) {
						for (PairDto pDto : result) {
							logger.info(pDto.getArg1() + "\t" + pDto.getRel()
									+ "\t" + pDto.getArg2());
							writer.write(pDto.getArg1() + "\t" + pDto.getRel()
									+ "\t" + pDto.getArg2() + "\t"
									+ pDto.getKbArg1() + "\t"
									+ pDto.getKbArg2() + "\n");
							writer.flush();
						}
					}
				}
			}
			Utilities.endTimer(start, i + " tasks finished in ");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// if (executorPool.isTerminated())
			// DBWrapper.shutDown();
		}
	}
}
