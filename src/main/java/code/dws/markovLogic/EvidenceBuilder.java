/**
 * 
 */

package code.dws.markovLogic;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Constants.OIE;
import code.dws.utils.Utilities;

/**
 * @author Arnab Dutta
 */
public class EvidenceBuilder {

	/**
	 * logger
	 */
	public final static Logger logger = LoggerFactory
			.getLogger(EvidenceBuilder.class);

	private String propertyName;
	private List<String> propertyNames;

	public static THashMap<String, Long> MAP_COUNTER = new THashMap<String, Long>();

	public static THashMap<String, List<String>> INSTANCE_CANDIDATES = new THashMap<String, List<String>>();
	// public static THashMap<String, List<String>> INSTANCE_CANDIDATES2 = new
	// THashMap<String, List<String>>();

	public static THashMap<String, List<String>> INSTANCE_TYPES = new THashMap<String, List<String>>();

	// The input OIE file with raw web extracted data
	public static File oieFile = null;

	public EvidenceBuilder(String[] args) throws IOException {

		if (Constants.IS_NELL) {
			this.propertyName = args[0];
			oieFile = new File(Constants.NELL_DATA_PATH);
			/**
			 * process the full NELL data dump
			 */
			this.processTriple(oieFile, OIE.NELL, ",");

		} else {
			if (!Constants.WORKFLOW_NORMAL) {
				// ReverbPropertyReNaming.main(new String[] { "" });
				//
				// // retrieve only the properties relavant to the given cluster
				// // name
				// this.propertyNames = ReverbPropertyReNaming
				// .getReNamedProperties().get(args[0]);
			} else {

				this.propertyNames = new ArrayList<String>();
				this.propertyNames
						.add(Constants.PREDICATE.replaceAll("-", " "));
			}

			oieFile = new File(Constants.OIE_DATA_PATH);

			/**
			 * process the full REVERB data dump
			 */
			this.processTriple(oieFile, OIE.REVERB, ";");
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length < 1)
			throw (new RuntimeException(
					"Usage : java -jar DS.jar <inputFilePath> <topK>"));
		else {
			// start processing the triples
			new EvidenceBuilder(args);

		}
	}

	/**
	 * process the OIE input file
	 * 
	 * @param inputFile
	 *            the raw file
	 * @param oieType
	 *            the type of OIE project, NELL or Reverb, different types have
	 *            different format
	 * @param delimit
	 *            delimiter, comma or tab or something else
	 * @throws IOException
	 */
	public void processTriple(File inputFile, OIE oieType, String delimit)
			throws IOException {

		String triple;
		String[] arrStr = null;

		THashMap<String, String> termConceptPair = new THashMap<String, String>();

		BufferedReader input = null;
		List<String> oieLines = null;

		// the file where the evidences for the MLN are written out
		BufferedWriter allEvidenceWriter = new BufferedWriter(new FileWriter(
				Constants.ALL_MLN_EVIDENCE));

		// BufferedWriter allEvidenceWriterTop1 = new BufferedWriter(
		// new FileWriter(Constants.ALL_MLN_EVIDENCE_T1));

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		if (Constants.IS_NELL) {
			input = new BufferedReader(new InputStreamReader(
					new FileInputStream(inputFile)));

			// iterate the file from OIE and process each triple at a time
			while ((triple = input.readLine()) != null) {

				// split on the delimiter
				arrStr = triple.split(delimit);

				// if the property is the one we want to sample upon
				if (this.propertyName != null && this.propertyName.length() > 0)
					if (this.propertyName.equals(arrStr[1])) {

						// process them
						this.createEvidences(arrStr[0], arrStr[1], arrStr[2],
								arrStr[3], allEvidenceWriter, termConceptPair);
					}

			}

		} else { // FOr REVERB

			for (String prop : this.propertyNames) {

				this.propertyName = prop;
				logger.info("Creating evidence for " + this.propertyName);
				input = new BufferedReader(new InputStreamReader(
						new FileInputStream(inputFile)));

				// iterate the file from OIE and process each triple at a time
				long s1 = Utilities.startTimer();
				oieLines = FileUtils.readLines(inputFile);

				// while ((triple = input.readLine()) != null) {
				for (String line : oieLines) {
					// split on the delimiter
					arrStr = line.split(delimit);

					// if the property is the one we want to sample upon
					if (this.propertyName.equals(arrStr[1])) {

						// process them
						this.createEvidences(arrStr[0], arrStr[1], arrStr[2],
								arrStr[3], allEvidenceWriter, termConceptPair);
					}
				}
				Utilities.endTimer(s1, "looping thru whole file = ");
			}
		}

		// remove from memory
		termConceptPair.clear();

		// close stream writer
		// allEvidenceWriterTop1.close();
		allEvidenceWriter.close();
		// goldEvidenceWriter.close();

		// flush residuals
		// DBWrapper.saveResidualDBPTypes();
		DBWrapper.saveResidualOIERefined();

		// shutdown DB
		DBWrapper.shutDown();

	}

	/**
	 * create the necessary evidence for running reasoning
	 * 
	 * @param sub
	 * @param prop
	 * @param obj
	 * @param conf
	 * @param oieType
	 * @param allEvidenceWriter
	 * @param termConceptPair
	 * @param goldEvidenceWriter
	 * @throws IOException
	 */
	private void createEvidences(String sub, String prop, String obj,
			String conf, BufferedWriter allEvidenceWriter,
			THashMap<String, String> termConceptPair) throws IOException {

		String oieSub = null;
		String oiePred = null;
		String oieObj = null;
		double confidence = 0;

		String oieSubPFxd = null;
		String oieObjPFxd = null;

		oieSub = Utilities.getInst(sub);
		oiePred = prop;
		oieObj = Utilities.getInst(obj);
		confidence = Double.parseDouble(conf);

		// uniquely identify each instance by concating a post fixd number
		oieSubPFxd = generateUniqueURI(oieSub.replaceAll("\\s+", "_"));
		oieObjPFxd = generateUniqueURI(oieObj.replaceAll("\\s+", "_"));

		// create a list of local mapping pair and return it
		// save to DB these post fixed mappings, needed later for property
		// discovery
		DBWrapper.saveToOIEPostFxd(oieSub, oiePred, oieObj, oieSubPFxd,
				oieObjPFxd);

		/**
		 * create the property assertions
		 */
		allEvidenceWriter.write("propAsstConf(\"NELL#Predicate/"
				+ oiePred.replaceAll("\\s+", "_") + "\", \"NELL#Instance/"
				+ Utilities.format(oieSubPFxd) + "\", \"NELL#Instance/"
				+ Utilities.format(oieObjPFxd) + "\", " + confidence + ")\n");

		/**
		 * create the property assertions
		 */
		// allEvidenceWriterTop1.write("propAsstConf(\"NELL#Predicate/"
		// + oiePred.replaceAll("\\s+", "_") + "\", \"NELL#Instance/"
		// + format(oieSubPFxd) + "\", \"NELL#Instance/"
		// + format(oieObjPFxd) + "\", " + confidence + ")\n");

		/**
		 * create top-k evidences for subject
		 */
		createEvidenceForTopKCandidates(allEvidenceWriter, oieSub, oieSubPFxd,
				termConceptPair, Constants.DOMAIN);

		/**
		 * create top-k evidences for object
		 */
		createEvidenceForTopKCandidates(allEvidenceWriter, oieObj, oieObjPFxd,
				termConceptPair, Constants.RANGE);

	}

	/**
	 * fetch the top-k instances and confidences for the subject
	 * 
	 * @param allEvidenceWriter
	 * @param oieInst
	 * @param oiePostFixdInst
	 * @param termConceptPair
	 * @param identifier
	 * @throws IOException
	 */
	public void createEvidenceForTopKCandidates(
			BufferedWriter allEvidenceWriter, String oieInst,
			String oiePostFixdInst, THashMap<String, String> termConceptPair,
			String identifier) throws IOException {

		DecimalFormat decimalFormatter = new DecimalFormat("0.00000000");

		List<String> sameAsConfidences;
		String conc;

		// get the top-k concepts, confidence pairs
		// UTF-8 at this stage
		sameAsConfidences = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
				.cleanse(oieInst).replaceAll("\\_+", " "),
				Constants.TOP_K_MATCHES);

		for (String val : sameAsConfidences) {

			// if one instance-dbpedia pair is already in, skip it
			if (!termConceptPair.containsKey(oiePostFixdInst + val)) {

				// back to character again
				conc = Utilities.utf8ToCharacter(val.split("\t")[0]);

				// the type info are written out to the writer object
				generateDBPediaTypeMLN(conc, allEvidenceWriter);

				conc = Utilities.removeTags("DBP#resource/"
						+ Utilities.characterToUTF8(conc.replaceAll("~", "%")));

				// write it out to the evidence file
				allEvidenceWriter.write("sameAsConf("
						+ conc
						+ ", \"NELL#Instance/"
						+ Utilities.format(oiePostFixdInst)
						+ "\", "
						+ decimalFormatter.format(Utilities
								.convertProbabilityToWeight(Double
										.parseDouble(val.split("\t")[1])))
						+ ")\n");

				termConceptPair.put(oiePostFixdInst + val, "");
			}
		}
	}

	/**
	 * create type of DBP instances
	 * 
	 * @param pair
	 * @param isOfTypeEvidenceWriter
	 * @param identifier
	 * @throws Exception
	 */
	private void generateDBPediaTypeMLN(String dbPediaInstance,
			BufferedWriter isOfTypeEvidenceWriter) {

		List<String> listTypes;

		String tempInst = null;

		tempInst = dbPediaInstance;

		// get DBPedia types
		if (Constants.RELOAD_TYPE)
			listTypes = SPARQLEndPointQueryAPI.getInstanceTypes(tempInst);

		else { // load cached copy from DB

			listTypes = DBWrapper.getDBPInstanceType(Utilities
					.characterToUTF8(tempInst));
		}

		try {
			if (listTypes.size() == 0
					|| listTypes.get(0).indexOf(Constants.UNTYPED) != -1) {

				isOfTypeEvidenceWriter.write("isOfType(\""
						+ Constants.UNTYPED
						+ "\", "
						+ Utilities.removeTags("DBP#resource/"
								+ Utilities.characterToUTF8(tempInst)) + ")\n");

				// if (Constants.RELOAD_TYPE)
				// DBWrapper.saveToDBPediaTypes(
				// Utilities.characterToUTF8(tempInst),
				// Constants.UNTYPED);

			} else if (listTypes.size() > 0) {

				// get the most specific type
				if (Constants.RELOAD_TYPE)
					listTypes = SPARQLEndPointQueryAPI.getLowestType(listTypes);

				for (String type : listTypes) {
					isOfTypeEvidenceWriter.write("isOfType(\"DBP#ontology/"
							+ type
							+ "\", "
							+ Utilities.removeTags("DBP#resource/"
									+ Utilities.characterToUTF8(tempInst))
							+ ")\n");

					// for faster future processing, store types in Database,
					// flip side is this may be old data, so change CONFIG
					// parameters to
					// reload fresh data and should be run once in a week or
					// so..
					// if (Constants.RELOAD_TYPE)
					// DBWrapper.saveToDBPediaTypes(
					// Utilities.characterToUTF8(tempInst), type);
				}
			}

		} catch (IOException e) {
			System.err.println("Exception in generateDBPediaTypeMLN() "
					+ e.getMessage());
		}
	}

	/**
	 * takes a nell/reverb instance and creates an unique URI out of it. So if
	 * multiple times an entity occurs, each one will have different uris.
	 * 
	 * @param nellInst
	 * @param classInstance
	 * @param elements2
	 * @param elements
	 * @return
	 */
	private static String generateUniqueURI(String nellInst) {
		// check if this URI is already there
		if (MAP_COUNTER.containsKey(nellInst)) {
			long value = MAP_COUNTER.get(nellInst);
			MAP_COUNTER.put(nellInst, value + 1);

			// create an unique URI because same entity already has been
			// encountered before
			nellInst = nellInst + Constants.POST_FIX
					+ String.valueOf(value + 1);

		} else {
			MAP_COUNTER.put(nellInst, 1L);
		}

		return nellInst;
	}

}
