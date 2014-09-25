/**
 * 
 */

package code.dws.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import code.dws.bootstrap.BootStrapMethod;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Constants.OIE;
import code.dws.utils.Utilities;

/**
 * This class takes a random triple from the OIE projects and computes the
 * Baseline for the subject and object. Uses the mapped candidates to predict
 * the type of the predicates.
 * 
 * @author Arnab Dutta
 */
public class DistantSupervised {
	static String[] pNames = { "bookwriter", "actorstarredinmovie",
			"bankbankincountry", "personleadsorganization",
			"athleteledsportsteam", "citylocatedinstate", "companyalsoknownas",
			"agentcollaborateswithagent", "teamplaysagainstteam",
			"weaponmadeincountry", "lakeinstate", "animalistypeofanimal"

	};

	long subjectCount = 0;
	long objectCount = 0;

	Map<String, Double> domainClassMap = new HashMap<String, Double>();
	Map<String, Double> rangeClassMap = new HashMap<String, Double>();

	String propertyName;
	List<String> propertyNames;

	// The input OIE file with raw web extracted data
	static File oieFile = null;

	/**
	 * @param propertyName
	 */
	public DistantSupervised(String[] args) {

		if (Constants.IS_NELL) {
			this.propertyName = args[0];
			oieFile = new File(Constants.NELL_DATA_PATH);
		} else {
			if (!Constants.WORKFLOW_NORMAL) {
				// this.propertyNames = ReverbPropertyReNaming
				// .getReNamedProperties().get(args[0]);
			} else {
				this.propertyNames = new ArrayList<String>();
				this.propertyNames
						.add(Constants.PREDICATE.replaceAll("-", " "));
			}
			oieFile = new File(Constants.OIE_DATA_PATH);
		}
		Constants.TOP_K_MATCHES = Integer.parseInt(args[1]);
	}

	/**
	 * stand alone test point
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		DistantSupervised dSup = null;

		if (args.length < 2)
			throw (new RuntimeException(
					"Usage : java -jar DS.jar <inputFilePath> <topK>"));
		else {
			// start processing the triples
			dSup = new DistantSupervised(args);
			dSup.learnDomRanFromFullData();

			System.out.println(dSup.getTopDomainClass(0));
			System.out.println(dSup.getTopRangeClass(0));
		}
	}

	/**
	 * use the entire file dump of OIE output to learn domain aand range values
	 * 
	 * @throws IOException
	 */
	public void learnDomRanFromFullData() throws IOException {

		if (Constants.ENGAGE_INTER_STEP) {
			BootStrapMethod
					.createSameAsHypotheses(Constants.BASIC_REASON_OUT_FILE);

			BootStrapMethod.createSubObjLists(Constants.BASIC_REASON_OUT_FILE);

			this.processTripleFromOutputFiles();
		} else {
			if (Constants.IS_NELL)
				this.processFullDataDump(oieFile, Constants.OIE.NELL, ",");
			else
				this.processFullDataDump(oieFile, Constants.OIE.REVERB, ";");
		}
	}

	/**
	 * use specific set of tuples to learn domain and range
	 * 
	 * @throws IOException
	 */
	public void learnDomRanFromFilteredOutputFiles() throws IOException {
		this.processTripleFromOutputFiles();
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
	public void processTripleFromOutputFiles() throws IOException {

		// here create the domain and range class map in a different fashion
		this.bootStrap();

		// the maps should have been updated
		domainClassMap = sortByValue(domainClassMap);
		rangeClassMap = sortByValue(rangeClassMap);

		System.out.println("DOMAIN MAP = " + domainClassMap);
		System.out.println("RANGE MAP = " + rangeClassMap);
	}

	/**
	 * read the reasoner produced output file for the set of sameAs statements
	 * 
	 * @param outputEvidenceFilePath
	 * @param oieType
	 * @throws IOException
	 */
	private void bootStrap() throws IOException {

		List<String> candidates = new ArrayList<String>();
		// List<String> entityTypes = null;

		boolean isSubject = false;

		// initiate DB
		DBWrapper.init(Constants.GET_WIKI_TITLES_SQL);

		String dbpinst = null;

		for (Pair<String, String> sameAsPair : BootStrapMethod.SAME_AS_LIST) {

			isSubject = BootStrapMethod.isSubject(sameAsPair.getRight());
			dbpinst = Utilities.cleanseInstances(sameAsPair.getLeft()
					.replaceAll("DBP#resource/", "").replaceAll("\"", ""));

			candidates.add(dbpinst);

			// call the DistantSupervised method for types and UPDATE the
			// domainClassMap and rangeClassMap
			if (isSubject)
				this.getTypes(candidates, Constants.DOMAIN);
			if (!isSubject)
				this.getTypes(candidates, Constants.RANGE);

			candidates.clear();
		}

		DBWrapper.saveResidualDBPTypes();
		DBWrapper.shutDown();

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
	public void processFullDataDump(File inputFile, OIE oieType, String delimit)
			throws IOException {

		String triple;
		String[] arrStr = null;

		BufferedReader input = null;

		// initiate DB
		DBWrapper.init(Constants.GET_WIKI_TITLES_SQL);

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
						this.processEachTriple(arrStr[0], arrStr[1], arrStr[2],
								oieType);
					}
			}
		} else {

			for (String prop : this.propertyNames) {

				this.propertyName = prop;

				System.out.println("Creating Alpha tree for "
						+ this.propertyName);
				input = new BufferedReader(new InputStreamReader(
						new FileInputStream(inputFile)));

				// iterate the file from OIE and process each triple at a time
				while ((triple = input.readLine()) != null) {

					// split on the delimiter
					arrStr = triple.split(delimit);

					// if the property is the one we want to sample upon
					if (this.propertyName != null
							&& this.propertyName.length() > 0)
						if (this.propertyName.equals(arrStr[1])) {

							// process them
							this.processEachTriple(arrStr[0], arrStr[1],
									arrStr[2], oieType);
						}
				}
			}
		}

		domainClassMap = sortByValue(domainClassMap);
		rangeClassMap = sortByValue(rangeClassMap);

		System.out.println("DOMAIN MAP = " + domainClassMap);
		System.out.println("RANGE MAP = " + rangeClassMap);

		DBWrapper.saveResidualDBPTypes();
		DBWrapper.shutDown();
	}

	/**
	 * overriden method.
	 * 
	 * @param sub
	 *            subject
	 * @param prop
	 *            property
	 * @param obj
	 *            object
	 * @param oieType
	 *            the type of OIE project, NELL or Reverb, different types have
	 *            diffeent format
	 */
	public void processEachTriple(String sub, String prop, String obj,
			OIE oieType) {

		// fetch the most probable matched entities both for subject and object
		List<String> subjConcepts = DBWrapper
				.fetchWikiTitles((oieType == Constants.OIE.NELL) ? Utilities
						.cleanse(sub).replaceAll("\\_+", " ") : Utilities
						.cleanse(sub).replaceAll("\\_+", " "));

		List<String> objConcepts = DBWrapper
				.fetchWikiTitles((oieType == Constants.OIE.NELL) ? Utilities
						.cleanse(obj).replaceAll("\\_+", " ") : Utilities
						.cleanse(obj).replaceAll("\\_+", " "));

		populateDomainAndRangeMaps(subjConcepts, objConcepts);

	}

	/**
	 * iterates the DBPEdia Entities and finds their types
	 * 
	 * @param subEntities
	 *            list of subject candidates
	 * @param objEntities
	 *            list of object candidates
	 */
	public void getTypes(List<String> entities, String domRanType) {
		List<String> entityTypes = null;

		double value = 0;

		// iterate over the subject entities
		for (String dbpEntity : entities) {
			dbpEntity = dbpEntity.replaceAll("\\s+", "_");

			// if(dbpEntity.indexOf("House_of_the_Seven_Gables") != -1)
			// System.out.println();

			if (Constants.RELOAD_TYPE) {
				entityTypes = SPARQLEndPointQueryAPI
						.getInstanceTypes(dbpEntity);
				entityTypes = SPARQLEndPointQueryAPI.getLowestType(entityTypes);
			} else {

				entityTypes = DBWrapper.getDBPInstanceType(Utilities
						.characterToUTF8(dbpEntity));
			}

			// if no entity types, add UNTYPED
			if (entityTypes.size() == 0
					|| entityTypes.get(0).indexOf(Constants.UNTYPED) != -1) {
				if (domRanType.equals(Constants.DOMAIN)) {
					if (domainClassMap.containsKey("UNTYPED")) {
						value = domainClassMap.get("UNTYPED");
						domainClassMap.put("UNTYPED", value + 1);
					} else {
						domainClassMap.put("UNTYPED", 1D);
					}
				} else {
					if (rangeClassMap.containsKey("UNTYPED")) {
						value = rangeClassMap.get("UNTYPED");
						rangeClassMap.put("UNTYPED", value + 1);
					} else {
						rangeClassMap.put("UNTYPED", 1D);
					}
				}

				// System.out.println("UNTYPED FOR " + dbpEntity);

				if (Constants.RELOAD_TYPE)
					DBWrapper.saveToDBPediaTypes(
							Utilities.characterToUTF8(dbpEntity),
							Constants.UNTYPED);

			} else { // normal processing

				for (String entityType : entityTypes) {

					if (domRanType.equals(Constants.DOMAIN)) {
						if (domainClassMap.containsKey(entityType)) {
							value = domainClassMap.get(entityType);
							domainClassMap.put(entityType, value + 1);
						} else {
							domainClassMap.put(entityType, 1D);
						}
					} else {
						if (rangeClassMap.containsKey(entityType)) {
							value = rangeClassMap.get(entityType);
							rangeClassMap.put(entityType, value + 1);
						} else {
							rangeClassMap.put(entityType, 1D);
						}
					}

					if (Constants.RELOAD_TYPE)
						DBWrapper.saveToDBPediaTypes(
								Utilities.characterToUTF8(dbpEntity),
								entityType);
				}
			}
		}
	}

	/**
	 * iterates the DBPEdia Entities and finds their types
	 * 
	 * @param subEntities
	 *            list of subject candidates
	 * @param objEntities
	 *            list of object candidates
	 */
	private void populateDomainAndRangeMaps(List<String> subEntities,
			List<String> objEntities) {
		List<String> entityTypes = null;

		double value = 0;

		// iterate over the subject entities
		for (String dbpEntity : subEntities) {
			dbpEntity = dbpEntity.replaceAll("\\s+", "_");

			if (Constants.RELOAD_TYPE) {
				entityTypes = SPARQLEndPointQueryAPI
						.getInstanceTypes(dbpEntity);
				entityTypes = SPARQLEndPointQueryAPI.getLowestType(entityTypes);
			} else { // load cached copy from DB

				entityTypes = DBWrapper.getDBPInstanceType(Utilities
						.characterToUTF8(dbpEntity));
			}

			if (entityTypes.size() == 0
					|| entityTypes.get(0).indexOf(Constants.UNTYPED) != -1) {
				if (domainClassMap.containsKey("UNTYPED")) {
					value = domainClassMap.get("UNTYPED");
					domainClassMap.put("UNTYPED", value + 1);
				} else {
					domainClassMap.put("UNTYPED", 1D);
				}

				if (Constants.RELOAD_TYPE)
					DBWrapper.saveToDBPediaTypes(
							Utilities.characterToUTF8(dbpEntity),
							Constants.UNTYPED);

			} else {

				for (String entityType : entityTypes) {
					if (domainClassMap.containsKey(entityType)) {
						value = domainClassMap.get(entityType);
						domainClassMap.put(entityType, value + 1);
					} else {
						domainClassMap.put(entityType, 1D);
					}

					if (Constants.RELOAD_TYPE)
						DBWrapper.saveToDBPediaTypes(
								Utilities.characterToUTF8(dbpEntity),
								entityType);
				}
			}
		}

		value = 0;

		// iterate over the object entities
		for (String dbpEntity : objEntities) {
			dbpEntity = dbpEntity.replaceAll("\\s+", "_");

			if (Constants.RELOAD_TYPE) {
				entityTypes = SPARQLEndPointQueryAPI
						.getInstanceTypes(dbpEntity);
				entityTypes = SPARQLEndPointQueryAPI.getLowestType(entityTypes);
			} else { // load cached copy from DB

				entityTypes = DBWrapper.getDBPInstanceType(Utilities
						.characterToUTF8(dbpEntity));
			}

			if (entityTypes.size() == 0
					|| entityTypes.get(0).indexOf(Constants.UNTYPED) != -1) {
				if (rangeClassMap.containsKey("UNTYPED")) {
					value = rangeClassMap.get("UNTYPED");
					rangeClassMap.put("UNTYPED", value + 1);
				} else {
					rangeClassMap.put("UNTYPED", 1D);
				}

				if (Constants.RELOAD_TYPE)
					DBWrapper.saveToDBPediaTypes(
							Utilities.characterToUTF8(dbpEntity),
							Constants.UNTYPED);

			} else {
				for (String entityType : entityTypes) {
					if (rangeClassMap.containsKey(entityType)) {
						value = rangeClassMap.get(entityType);
						rangeClassMap.put(entityType, value + 1);
					} else {
						rangeClassMap.put(entityType, 1D);
					}

					if (Constants.RELOAD_TYPE)
						DBWrapper.saveToDBPediaTypes(
								Utilities.characterToUTF8(dbpEntity),
								entityType);

				}
			}
		}
	}

	/**
	 * sort a map by value descending
	 * 
	 * @param map
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String, Double> sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();

			// result.put(entry.getKey(), (Double) entry.getValue()/
			// (tripleCounter * Constants.TOP_ANCHORS));
			result.put(entry.getKey(), (Double) entry.getValue());
		}
		return result;
	}

	/**
	 * @return the typeCountSubjectMap
	 */
	public Map<String, Double> getAllSubjectTypes() {
		return domainClassMap;
	}

	/**
	 * @return the typeCountObjectsMap
	 */
	public Map<String, Double> getAllObjectTypes() {
		return rangeClassMap;
	}

	/**
	 * get top entry of Domain
	 * 
	 * @param topK
	 * @return
	 */
	public List<Pair<String, Double>> getTopDomainClass(int topK) {
		List<Pair<String, Double>> listTopKClasses = new ArrayList<Pair<String, Double>>();

		for (Map.Entry<String, Double> entry : domainClassMap.entrySet()) {
			if (topK != 0 && listTopKClasses.size() == topK)
				return listTopKClasses;
			else {
				listTopKClasses.add(new ImmutablePair<String, Double>(entry
						.getKey(), entry.getValue()));
			}
		}

		return listTopKClasses;
	}

	/**
	 * get top entry of Range
	 * 
	 * @param topK
	 * @return
	 */
	public List<Pair<String, Double>> getTopRangeClass(int topK) {
		List<Pair<String, Double>> listTopKClasses = new ArrayList<Pair<String, Double>>();

		for (Map.Entry<String, Double> entry : rangeClassMap.entrySet()) {
			if (topK != 0 && listTopKClasses.size() == topK)
				return listTopKClasses;
			else
				listTopKClasses.add(new ImmutablePair<String, Double>(entry
						.getKey(), entry.getValue()));
		}

		return listTopKClasses;
	}

}
