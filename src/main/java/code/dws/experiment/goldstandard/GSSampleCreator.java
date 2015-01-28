/**
 * 
 */
package code.dws.experiment.goldstandard;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * @author adutta
 *
 */
public class GSSampleCreator {

	// define Logger
	public static Logger logger = Logger.getLogger(GSSampleCreator.class
			.getName());

	private static Map<String, List<List<String>>> ANNO_PROPS = new HashMap<String, List<List<String>>>();

	/**
	 * 
	 */
	public GSSampleCreator() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// load constants file
		Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });

		String location = new File(Constants.OIE_DATA_PATH).getParent();

		// load the annotated OIE relations
		loadAnnotatedProperties();

		// load the fminus File, randomly sampling lines
		try {
			sampleFile(location);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	private static void loadAnnotatedProperties() {

		// init the DB
		DBWrapper.init(Constants.GET_OIE_PROPERTIES_ANNOTATED);
		ANNO_PROPS = DBWrapper.getAnnoPairs();

		for (Map.Entry<String, List<List<String>>> e : ANNO_PROPS.entrySet()) {
			for (List<String> lst : e.getValue()) {
				logger.debug(e.getKey() + "\t" + lst);
			}
		}
		DBWrapper.shutDown();
	}

	/**
	 * sample for the triples which should be used for annotation
	 * 
	 * @param directory
	 * @throws IOException
	 */
	private static void sampleFile(String directory) throws IOException {
		String line = null;
		Scanner scan = null;
		String oieSub = null;
		String oieRel = null;
		String oieObj = null;

		String[] elems = null;

		List<String> candidateSubjs = null;
		List<String> candidateObjs = null;

		try {
			scan = new Scanner(new File(directory + "/fMinus.dat"), "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		BufferedWriter goldFile = new BufferedWriter(new FileWriter(directory
				+ "/GOLD.tsv"));

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// iterate the file
		while (scan.hasNextLine()) {
			line = scan.nextLine();
			elems = line.split(Constants.OIE_DATA_SEPERARTOR);

			// valid line which can be used for evaluation
			if (ANNO_PROPS.containsKey(elems[1])) {

				logger.debug(line);

				oieSub = elems[0];
				oieRel = elems[1];
				oieObj = elems[2];

				// get the top-k concepts for the subject
				candidateSubjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
						.cleanse(oieSub).replaceAll("\\_+", " ").trim(),
						Constants.TOP_K_MATCHES);

				// get the top-k concepts for the object
				candidateObjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
						.cleanse(oieObj).replaceAll("\\_+", " ").trim(),
						Constants.TOP_K_MATCHES);

				writeOut(line, candidateSubjs, candidateObjs, oieSub, oieRel,
						oieObj, goldFile);
			}
		}

		if (goldFile != null)
			goldFile.close();

		DBWrapper.shutDown();
	}

	/**
	 * write out in a way it is convenient to annotate
	 * 
	 * @param line
	 * @param candidateSubjs
	 * @param candidateObjs
	 * @param goldFile
	 * @param possibleValues
	 * @throws IOException
	 */
	private static void writeOut(String line, List<String> candidateSubjs,
			List<String> candidateObjs, String oieSub, String oieRel,
			String oieObj, BufferedWriter goldFile) throws IOException {

		// header section
		goldFile.write(oieSub + "\t" + oieRel + "\t" + oieObj + "\t" + ""
				+ "\t" + "" + "\t\n");

		int depth = (ANNO_PROPS.get(oieRel).size() > Constants.TOP_K_MATCHES) ? ANNO_PROPS
				.get(oieRel).size() : Constants.TOP_K_MATCHES;

		// iterate the candidates and write out the options
		for (int i = 0; i < depth; i++) {
			String candSub = (i >= candidateSubjs.size()) ? "" : candidateSubjs
					.get(i).split("\t")[0];
			String candObj = (i >= candidateObjs.size()) ? "" : candidateObjs
					.get(i).split("\t")[0];
			String candRel = (i >= ANNO_PROPS.get(oieRel).size()) ? ""
					: ANNO_PROPS.get(oieRel).get(i).get(0);

			goldFile.write("\t\t\t" + Utilities.utf8ToCharacter(candSub) + "\t"
					+ candRel + "\t" + Utilities.utf8ToCharacter(candObj)
					+ "\n");
		}
		// a line separator
		goldFile.write("\n");
		goldFile.flush();
	}
}
