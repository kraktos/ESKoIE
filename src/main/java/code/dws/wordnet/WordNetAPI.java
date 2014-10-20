package code.dws.wordnet;

import java.util.ArrayList;
import java.util.List;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.HirstStOnge;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.impl.Lesk;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.cmu.lti.ws4j.impl.Path;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.smu.tspell.wordnet.Synset;
import edu.smu.tspell.wordnet.WordNetDatabase;
import edu.smu.tspell.wordnet.WordNetException;

/**
 * Displays word forms and definitions for synsets containing the word form
 * specified on the command line. To use this application, specify the word form
 * that you wish to view synsets for, as in the following example which displays
 * all synsets containing the word form "airplane": <br>
 * java TestJAWS airplane
 */
public class WordNetAPI {
	// static {
	// System.setProperty("wordnet.database.dir",
	// "/home/adutta/WordNet-3.0/dict/");
	// WS4JConfiguration.getInstance().setMFS(true);
	//
	// }

	/**
	 * 
	 */
	public WordNetAPI() {

		// TODO Auto-generated constructor stub
	}

	/**
	 * Main entry point. The command-line arguments are concatenated together
	 * (separated by spaces) and used as the word form to look up.
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ILexicalDatabase db = new NictWordNet();

		// new HirstStOnge(db)

		// new WuPalmer(db), new HirstStOnge(db),
		// new LeacockChodorow(db), new Lesk(db), new Resnik(db),
		// new JiangConrath(db), new Lin(db), new Path(db)

		RelatednessCalculator[] rcs = { new WuPalmer(db) };

		System.out.println(scoreWordNet(rcs, "is a parent of".split(" "),
				"is a father of".split(" "))
				+ "\t"
				+ SimilatityWebService.getSimScore("is a parent of",
						"is a father of"));

		System.out.println(scoreWordNet(rcs, "is a town in".split(" "),
				"is awarded to".split(" "))
				+ "\t"
				+ SimilatityWebService.getSimScore("is a town in",
						"is awarded to"));

		System.out.println(scoreWordNet(rcs, "is a town in".split(" "),
				"is a parent of".split(" "))
				+ "\t"
				+ SimilatityWebService.getSimScore("is a town in",
						"is a parent of"));

		System.out.println(scoreWordNet(rcs, "is a town in".split(" "),
				"is a suburb".split(" "))
				+ "\t"
				+ SimilatityWebService.getSimScore("is a town in",
						"is a suburb"));

		System.out.println(scoreWordNet(rcs, "studied at".split(" "),
				"has studied at".split(" "))
				+ "\t"
				+ SimilatityWebService.getSimScore("has studied at",
						"studied at"));

	}

	/**
	 * @param wordForm
	 * @return
	 * @throws WordNetException
	 */
	public static List<String> getSynonyms(String wordForm)
			throws WordNetException {
		List<String> result = null;

		// Get the synsets containing the wrod form
		WordNetDatabase database = WordNetDatabase.getFileInstance();

		Synset[] synsets = database.getSynsets(wordForm);

		// Display the word forms and definitions for synsets retrieved
		if (synsets.length > 0) {

			result = new ArrayList<String>();

			for (int i = 0; i < synsets.length; i++) {

				String[] wordForms = synsets[i].getWordForms();
				for (int j = 0; j < wordForms.length; j++) {

					result.add(wordForms[j]);
					// System.out.print((j > 0 ? ", " : "") + wordForms[j]);
				}
				// System.out.println(": " + synsets[i].getDefinition());
			}
		}

		return (result != null) ? result : new ArrayList<String>();
	}

	/**
	 * score two strings similarity
	 * 
	 * @param rcs
	 * 
	 * @param strArg1
	 * @param strArg2
	 * @return
	 */
	public static double scoreWordNet(RelatednessCalculator[] rcs,
			String[] strArg1, String[] strArg2) {
		double score = 0;
		int dnm = 0;

		double[][] d = rcs[0].getNormalizedSimilarityMatrix(strArg1, strArg2);
//		double[][] d2 = rcs[1].getNormalizedSimilarityMatrix(strArg1, strArg2);
//		double[][] d3 = rcs[2].getNormalizedSimilarityMatrix(strArg1, strArg2);
		// double[][] d4 = rcs[3].getNormalizedSimilarityMatrix(strArg1,
		// strArg2);
		// double[][] d5 = rcs[4].getNormalizedSimilarityMatrix(strArg1,
		// strArg2);
		// double[][] d6 = rcs[5].getNormalizedSimilarityMatrix(strArg1,
		// strArg2);
		// double[][] d7 = rcs[6].getNormalizedSimilarityMatrix(strArg1,
		// strArg2);
		// double[][] d8 = rcs[7].getNormalizedSimilarityMatrix(strArg1,
		// strArg2);

		for (int row = 0; row < d.length; row++) {
			for (int column = 0; column < d[row].length; column++) {
				if (d[row][column] > 0) {
					score = score
							+ (double) (d[row][column] )
							/ 1;

					dnm++;
				}
			}
		}

		return (score > 0) ? (double) score / dnm : 0;

	}
}
