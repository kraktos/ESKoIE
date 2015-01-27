/**
 * 
 */
package code.dws.experiment.goldstandard;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.knowGen.GenerateNewProperties;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * This file, splits the input OIE data set into F+ and F-, see paper for
 * details F+ = facts with analogous DB facts assertions F- = facts without
 * analogous DB facts assertions
 * 
 * F- are potential knowledge generation source
 * 
 * @author adutta
 *
 */
public class OIEFactSeperator {

	// define Logger
	public static Logger log = Logger.getLogger(GenerateNewProperties.class
			.getName());
	private static long missed = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// load constants file
		Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });

		// load OIE data file in memory and generate F+ and F- files
		readOieFile(Constants.OIE_DATA_PATH);

	}

	/**
	 * load the OIE data file in memory and read every line
	 * 
	 * @param oieFilePath
	 */
	private static void readOieFile(String oieFilePath) {
		String line = null;
		String oieRawSubj = null;
		String oieRawObj = null;

		String[] elems = null;
		BufferedWriter factPlusWriter = null;
		BufferedWriter factMinusWriter = null;

		List<String> candidateSubjs = null;
		List<String> candidateObjs = null;

		String directory = new File(oieFilePath).getParent().toString();

		try {
			factPlusWriter = new BufferedWriter(new FileWriter(directory
					+ "/fPlus.dat"));

			factMinusWriter = new BufferedWriter(new FileWriter(directory
					+ "/fMinus.dat"));
		} catch (IOException e1) {
			log.error("Problem creating file writer ");
		}

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		Scanner scan = null;
		try {
			scan = new Scanner(new File(oieFilePath), "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// iterate the file
		while (scan.hasNextLine()) {
			line = scan.nextLine();

			elems = line.split(Constants.OIE_DATA_SEPERARTOR);
			try {
				log.debug(elems[0] + "\t" + elems[1] + "\t" + elems[2] + "\n");

				// get the nell subjects and objects
				oieRawSubj = elems[0];
				oieRawObj = elems[2];

				// get the top-k concepts for the subject
				candidateSubjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
						.cleanse(oieRawSubj).replaceAll("\\_+", " ").trim(),
						Constants.TOP_K_MATCHES);

				// get the top-k concepts for the object
				candidateObjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
						.cleanse(oieRawObj).replaceAll("\\_+", " ").trim(),
						Constants.TOP_K_MATCHES);

				findInKB(line, candidateSubjs, candidateObjs, factPlusWriter,
						factMinusWriter);

			} catch (Exception ex) {
				missed++;
				log.error("Problem with line " + line + "\t; mised = " + missed);
			}
		}
	}

	/**
	 * look in the KB, DBPEDIA, if the assertion exists
	 * 
	 * @param line
	 * @param candidateSubjs
	 * @param candidateObjs
	 * @param factPlusWriter
	 * @param factMinusWriter
	 */
	private static void findInKB(String line, List<String> candidateSubjs,
			List<String> candidateObjs, BufferedWriter factPlusWriter,
			BufferedWriter factMinusWriter) {

		boolean hasInKB = false;

		for (String subjCand : candidateSubjs) {
			for (String objCand : candidateObjs) {

				hasInKB = getAssertionFromEndpoint(subjCand.split("\t")[0],
						objCand.split("\t")[0]);
				if (hasInKB)
					break;
			}
			if (hasInKB)
				break;
		}

		try {
			if (hasInKB) {
				factPlusWriter.write(line + "\n");
				factPlusWriter.flush();
			} else {
				factMinusWriter.write(line + "\n");
				factMinusWriter.flush();
			}
		} catch (IOException e) {
			log.error("Problem with finding in KB " + e.getMessage());
		}

	}

	/**
	 * query KB endpoint, for the existence of any such triple
	 * 
	 * @param candSubj
	 * @param candObj
	 * @return
	 */
	private static boolean getAssertionFromEndpoint(String candSubj,
			String candObj) {

		// remove all utf-8 characters and convert them to characters
		candSubj = Utilities.utf8ToCharacter(candSubj);
		candObj = Utilities.utf8ToCharacter(candObj);

		if (candSubj.endsWith("%"))
			candSubj = candSubj.replaceAll("%", "");

		if (candObj.endsWith("%"))
			candObj = candObj.replaceAll("%", "");

		String sparqlQuery = "select * where {<"
				+ Constants.DBPEDIA_INSTANCE_NS
				+ candSubj
				+ "> ?val <"
				+ Constants.DBPEDIA_INSTANCE_NS
				+ candObj
				+ ">. "
				+ "?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>."
				+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageWikiLink'))}";

		log.debug(sparqlQuery);

		// fetch the result set
		List<QuerySolution> listResults = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(sparqlQuery);

		// debug point if interested in seeing the assertion in KB
		// for (QuerySolution querySol : listResults) {
		// possiblePred = querySol.get("val").toString();
		//
		// log.info(candSubj + "\t" + possiblePred + "\t" + candObj);
		// }

		if (listResults.size() > 0)
			return true;
		else
			return false;

	}
}
