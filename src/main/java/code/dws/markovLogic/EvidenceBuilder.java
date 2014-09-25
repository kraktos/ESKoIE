/**
 * 
 */

package code.dws.markovLogic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Constants.OIE;
import code.dws.utils.Utilities;

/**
 * @author Arnab Dutta
 */
public class EvidenceBuilder {

	private String propertyName;
	private List<String> propertyNames;

	public static Map<String, Long> MAP_COUNTER = new HashMap<String, Long>();

	// The input OIE file with raw web extracted data
	static File oieFile = null;

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

		Set<String> termConceptPairSet = new HashSet<String>();

		// initiate DB
		// DBWrapper.init(Constants.GET_WIKI_TITLES_SQL);

		BufferedReader input = null;

		// the file where the evidences for the MLN are written out
		BufferedWriter allEvidenceWriter = new BufferedWriter(new FileWriter(
				Constants.ALL_MLN_EVIDENCE));

		BufferedWriter allEvidenceWriterTop1 = new BufferedWriter(
				new FileWriter(Constants.ALL_MLN_EVIDENCE_T1));

		// the file where the evidences for the MLN are written out
		// BufferedWriter goldEvidenceWriter = new BufferedWriter(new
		// FileWriter(
		// Constants.GOLD_MLN_EVIDENCE_ALL));

		// load the gold standard file.
		// loadGoldStandard();

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
								arrStr[3], allEvidenceWriterTop1,
								allEvidenceWriter, termConceptPairSet);
					}

			}

		} else { // FOr REVERB

			for (String prop : this.propertyNames) {

				this.propertyName = prop;

				System.out
						.println("Creating evidence for " + this.propertyName);

				input = new BufferedReader(new InputStreamReader(
						new FileInputStream(inputFile)));

				// iterate the file from OIE and process each triple at a time
				while ((triple = input.readLine()) != null) {

					// split on the delimiter
					arrStr = triple.split(delimit);

					// if the property is the one we want to sample upon
					if (this.propertyName.equals(arrStr[1])) {

						// process them
						this.createEvidences(arrStr[0], arrStr[1], arrStr[2],
								arrStr[3], allEvidenceWriterTop1,
								allEvidenceWriter, termConceptPairSet);
					}
				}
			}
		}

		// remove from memory
		termConceptPairSet.clear();

		// close stream writer
		allEvidenceWriterTop1.close();
		allEvidenceWriter.close();
		// goldEvidenceWriter.close();

		// flush residuals
		DBWrapper.saveResidualDBPTypes();
		DBWrapper.saveResidualOIERefined();

		// shutdown DB
		DBWrapper.shutDown();

	}

	private static String format(String arg) {
		return arg.replaceAll(",", "~2C").replaceAll("\\$", "~24")
				.replaceAll("%", "~25");
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
	 * @param termConceptPairSet
	 * @param goldEvidenceWriter
	 * @throws IOException
	 */
	private void createEvidences(String sub, String prop, String obj,
			String conf, BufferedWriter allEvidenceWriterTop1,
			BufferedWriter allEvidenceWriter, Set<String> termConceptPairSet)
			throws IOException {

		String nellSub = null;
		String nellPred = null;
		String nellObj = null;
		double confidence = 0;

		String nellSubPFxd = null;
		String nellObjPFxd = null;

		nellSub = Utilities.getInst(sub);
		nellPred = prop;
		nellObj = Utilities.getInst(obj);
		confidence = Double.parseDouble(conf);

		// uniquely identify each instance by concating a post fixd number
		nellSubPFxd = generateUniqueURI(nellSub.replaceAll("\\s+", "_"));
		nellObjPFxd = generateUniqueURI(nellObj.replaceAll("\\s+", "_"));

		// create a list of local mapping pair and return it
		// save to DB these post fixed mappings, needed later for property
		// discovery
		saveToDB(nellSub, nellPred, nellObj, nellSubPFxd, nellObjPFxd);

		/**
		 * create the property assertions
		 */
		allEvidenceWriter.write("propAsstConf(\"NELL#Predicate/"
				+ nellPred.replaceAll("\\s+", "_") + "\", \"NELL#Instance/"
				+ format(nellSubPFxd) + "\", \"NELL#Instance/"
				+ format(nellObjPFxd) + "\", " + confidence + ")\n");

		/**
		 * create the property assertions
		 */
		allEvidenceWriterTop1.write("propAsstConf(\"NELL#Predicate/"
				+ nellPred.replaceAll("\\s+", "_") + "\", \"NELL#Instance/"
				+ format(nellSubPFxd) + "\", \"NELL#Instance/"
				+ format(nellObjPFxd) + "\", " + confidence + ")\n");

		/**
		 * create top-k evidences for subject
		 */
		createEvidenceForTopKCandidates(allEvidenceWriterTop1,
				allEvidenceWriter, nellSub, nellSubPFxd, termConceptPairSet,
				Constants.DOMAIN);

		/**
		 * create top-k evidences for object
		 */
		createEvidenceForTopKCandidates(allEvidenceWriterTop1,
				allEvidenceWriter, nellObj, nellObjPFxd, termConceptPairSet,
				Constants.RANGE);

	}

	/**
	 * fetch the top-k instances and confidences for the subject
	 * 
	 * @param allEvidenceWriter
	 * @param nellInst
	 * @param nellPostFixdInst
	 * @param termConceptPairSet
	 * @param identifier
	 * @return
	 * @throws IOException
	 */
	public List<String> createEvidenceForTopKCandidates(
			BufferedWriter allEvidenceWriterTop1,
			BufferedWriter allEvidenceWriter, String nellInst,
			String nellPostFixdInst, Set<String> termConceptPairSet,
			String identifier) throws IOException {

		DecimalFormat decimalFormatter = new DecimalFormat("0.00000000");

		List<String> sameAsConfidences;
		String conc;

		// get the top-k concepts, confidence pairs
		// UTF-8 at this stage
		sameAsConfidences = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
				.cleanse(nellInst).replaceAll("\\_+", " "),
				Constants.TOP_K_MATCHES);

		List<String> listMappings = new ArrayList<String>();

		for (String val : sameAsConfidences) {

			// if one instance-dbpedia pair is already in, skip it
			if (!termConceptPairSet.contains(nellPostFixdInst + val)) {

				// back to character again
				conc = Utilities.utf8ToCharacter(val.split("\t")[0]);

				// if (conc.indexOf("We_are_the_99") != -1)
				// System.out.println();

				generateDBPediaTypeMLN(conc, allEvidenceWriter);

				conc = Utilities.removeTags("DBP#resource/"
						+ Utilities.characterToUTF8(conc.replaceAll("~", "%")));

				// write it out to the evidence file
				allEvidenceWriter.write("sameAsConf("
						+ conc
						+ ", \"NELL#Instance/"
						+ format(nellPostFixdInst)
						+ "\", "
						+ decimalFormatter.format(Utilities
								.convertProbabilityToWeight(Double
										.parseDouble(val.split("\t")[1])))
						+ ")\n");

				listMappings.add(conc);

				termConceptPairSet.add(nellPostFixdInst + val);
			}
		}

		if (sameAsConfidences.size() > 0) {
			// write it out to the evidence file

			conc = Utilities.utf8ToCharacter(sameAsConfidences.get(0).split(
					"\t")[0]);
			conc = Utilities.removeTags("DBP#resource/"
					+ Utilities.characterToUTF8(conc.replaceAll("~", "%")));

			allEvidenceWriterTop1.write("sameAsConf("
					+ conc
					+ ", \"NELL#Instance/"
					+ nellPostFixdInst
					+ "\", "
					+ decimalFormatter.format(Utilities
							.convertProbabilityToWeight(Double
									.parseDouble(sameAsConfidences.get(0)
											.split("\t")[1]))) + ")\n");
		}

		// cache the type information for the top most candidate, the 0th
		// element is the most frequent candidate
		// cacheType(sameAsConfidences.get(0), identifier);

		return listMappings;
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

				if (Constants.RELOAD_TYPE)
					DBWrapper.saveToDBPediaTypes(
							Utilities.characterToUTF8(tempInst),
							Constants.UNTYPED);

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
					if (Constants.RELOAD_TYPE)
						DBWrapper.saveToDBPediaTypes(
								Utilities.characterToUTF8(tempInst), type);
				}
			}

		} catch (IOException e) {
			System.err.println("Exception in generateDBPediaTypeMLN() "
					+ e.getMessage());
		}
	}

	private void saveToDB(String oieSub, String oiePred, String oieObj,
			String oieSubPfxd, String oieObjPfxd) {
		DBWrapper.saveToOIEPostFxd(oieSub, oiePred, oieObj, oieSubPfxd,
				oieObjPfxd);
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
