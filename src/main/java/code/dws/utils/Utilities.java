/**
 * 
 */

package code.dws.utils;

import java.io.BufferedWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * All different kinds of utility methods are placed here
 * 
 * @author Arnab Dutta
 */
public class Utilities {
	// define Logger
	public static Logger logger = Logger.getLogger(Utilities.class.getName());

	public static HashSet<String> dict = new HashSet<String>();

	public static int countGlbl = 10;

	public static Set<String> set = new TreeSet<String>();

	// define Logger

	static Set<Long> UNIQUE_PROPERTIES = new HashSet<Long>();

	// set of stop words
	static final Set<String> STOP_WORDS = new HashSet<String>(Arrays.asList(
			"a", "the", "an", "of"));

	/**
	 * Prints a map
	 * 
	 * @param map
	 */
	public static void printMap(Map<?, ?> map) {
		for (Iterator<?> it = map.entrySet().iterator(); it.hasNext();) {
			Map.Entry<?, ?> entry = (Entry<?, ?>) it.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
			logger.info(key + "  " + value);
		}
	}

	/**
	 * Iterate the list and print out the string literals for the query
	 * 
	 * @param resultList
	 * @param out
	 */
	public static void printList(List<Long> resultList, BufferedWriter out) {
		try {
			for (int listCounter = 0; listCounter < resultList.size(); listCounter++) {
				// print only the odd values
				if (listCounter % 2 != 0) {
					// this gives a set of properties for the given query
					UNIQUE_PROPERTIES.add(resultList.get(listCounter));
				}
			}
			logger.info("Unique properties  = " + UNIQUE_PROPERTIES.size()
					+ "\n");
		} finally {
			UNIQUE_PROPERTIES.clear();
		}

	}

	/**
	 * Prints a set
	 * 
	 * @param set
	 */
	public static void printSet(final Set<?> set) {
		Iterator<?> it = set.iterator();
		while (it.hasNext()) {
			logger.info(it.next().toString());
		}
	}

	/**
	 * @param start
	 *            the timer start point
	 * @param message
	 *            the message you want to display
	 */
	public static void endTimer(final long start, final String message) {
		long end = System.currentTimeMillis();
		long execTime = end - start;
		logger.debug(message
				+ " "
				+ String.format("%02d ms",
						TimeUnit.MILLISECONDS.toMillis(execTime)));
	}

	/**
	 * @return the start point of time
	 */
	public static long startTimer() {
		long start = System.currentTimeMillis();
		return start;
	}

	public static void printList(List<?> resultList) {
		for (int listCounter = 0; listCounter < resultList.size(); listCounter++) {
			logger.info(resultList.get(listCounter).toString());
		}
	}

	public static void split(String head, String in) {

		// System.out.println(head + "  " + in);
		// head + " " + in is a segmentation
		String segment = head + " " + in;

		// count number of dictionary words
		int count = 0;
		Scanner phraseScan = new Scanner(segment);
		while (phraseScan.hasNext()) {
			String word = phraseScan.next();
			if (dict.contains(word))
				count++;
		}

		if (count < countGlbl) {
			if (set.size() > 0) {
				set.clear();
			}
			set.add(segment);
		}

		if (count == 4)
			System.out.println(segment + "\t" + count + " English words");

		// recursive calls
		for (int i = 2; i < in.length(); i++) {
			split(head + " " + in.substring(0, i), in.substring(i, in.length()));
		}

	}

	public static String prun(String uri) {
		String s = uri.substring(uri.lastIndexOf("/") + 1, uri.length());
		if (s.indexOf(":") != -1)
			s = s.replaceAll(":", "");

		return s;
	}

	public static String cleanse(String arg) {
		arg = arg.substring(arg.lastIndexOf(":") + 1, arg.length());
		return arg.toLowerCase();
	}

	/**
	 * oie instance cleaning
	 * 
	 * @param arg
	 * @return
	 */
	public static String clean(String arg) {
		if (arg.indexOf(":") != -1)
			arg = arg.substring(arg.indexOf(":") + 1, arg.length());

		return arg.replaceAll("\\_+", " ");
	}

	public static String removeStopWords(String originalWord) {
		StringBuffer retVal = new StringBuffer();
		String[] arrWords = originalWord.split(" ");
		for (String word : arrWords) {
			if (!STOP_WORDS.contains(word.toLowerCase())) {
				retVal.append(word + " ");
			}
		}

		return retVal.toString().toLowerCase().trim();
	}

	/**
	 * encodes a string with special character to one with UTF-8 encoding
	 * 
	 * @param arg
	 * @return
	 */
	public static String characterToUTF8(String arg) {
		try {
			if (arg == null)
				return arg;
			return URLEncoder.encode(arg, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.info("Exception while encoding");
		}
		return arg;
	}

	/**
	 * decodes a string with UTF-8 encoding to special character
	 * 
	 * @param arg
	 * @return
	 */
	public static String utf8ToCharacter(String arg) {
		try {
			if (arg == null)
				return arg;
			return URLDecoder.decode(arg, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.info("Exception while dencoding");
			e.printStackTrace();
		} catch (IllegalArgumentException e2) {
			logger.info("Exception while dencoding");
			e2.printStackTrace();
		}
		return arg;
	}

	/**
	 * converts a probability to weights with smoothing
	 * 
	 * @param prob
	 * @return
	 */
	public static double convertProbabilityToWeight(double prob) {
		if (Constants.USE_LOGIT) {
			// smoothing
			if (prob >= 1)
				prob = 1 - Math.pow(10, -6);
			if (prob <= 0)
				prob = 0 + Math.pow(10, -6);

			return Constants.SCALE_WEIGHT + Math.log(prob / (1 - prob));
		} else
			return prob;
	}

	// public static Map sortByValue(Map map) {
	// List list = new LinkedList(map.entrySet());
	// Collections.sort(list, new Comparator() {
	// public int compare(Object o2, Object o1) {
	// return ((Comparable) ((Map.Entry) (o1)).getValue())
	// .compareTo(((Map.Entry) (o2)).getValue());
	// }
	// });
	//
	// Map result = new LinkedHashMap();
	// for (Iterator it = list.iterator(); it.hasNext();) {
	// Map.Entry entry = (Map.Entry) it.next();
	// result.put(entry.getKey(), entry.getValue());
	// }
	// return result;
	// }

	// ***************************************************************
	/**
	 * removes the DBpedia header uri information and cleanes the concept from
	 * any special character by converting it to to UTF-8
	 * 
	 * @param arg
	 * @return
	 */
	public static String cleanDBpediaURI(String arg) {
		return arg.replaceAll(Constants.DBPEDIA_PREDICATE_NS, "")
				.replaceAll(Constants.DBPEDIA_INSTANCE_NS, "")
				.replaceAll("\"", ""); // TODO
		// replaceAll(":_", "__")
	}

	public static String cleanYagoURI(String arg) {
		return arg.replaceAll(Constants.YAGO_HEADER + "/", "")
				.replaceAll(Constants.DBPEDIA_INSTANCE_NS, "")
				.replaceAll("\"", ""); // TODO
		// replaceAll(":_", "__")
	}

	public static String cleanForMLNPresentation(String arg) {
		arg = arg.replaceAll("(", "[");
		arg = arg.replaceAll(")", "]");
		arg = arg.replaceAll("&", "~26");

		return arg;
	}

	public static String cleanTerms(String arg) {
		arg = arg.replaceAll("\"", "").trim();
		arg = arg.replaceAll("http://dbpedia.org/resource/", "");
		return arg;
	}

	public static String cleanseInstances(String dbpInst) {
		dbpInst = dbpInst.replaceAll("~", "%");
		dbpInst = dbpInst.replaceAll("\\[", "(");
		dbpInst = dbpInst.replaceAll("\\]", ")");
		dbpInst = dbpInst.replaceAll("\\*", "'");
		return utf8ToCharacter(dbpInst);
	}

	/**
	 * sort a map by value descending
	 * 
	 * @param map
	 * @param totalScore
	 * @param tripleCounter
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String, Long> sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String, Long> sortByValue(Map map, Long cutOff) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = (Map.Entry) it.next();
			if (entry.getValue() >= cutOff)
				result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	/**
	 * get the actual nell instance, following the ":" if any
	 * 
	 * @param arg
	 * @param identifier
	 * @return
	 */
	public static String getInst(String arg) {

		if (arg.indexOf(":") != -1)
			return arg.substring(arg.indexOf(":") + 1, arg.length());
		else
			return arg;
	}

	/**
	 * cleans of the "<" or ">" on the concepts
	 * 
	 * @param arg
	 *            value to be cleaned
	 * @return
	 */
	public static String removeTags(String arg) {

		arg = arg.replaceAll("_:", "");
		arg = arg.replaceAll("<", "");
		arg = arg.replaceAll(">\\)", "");
		arg = arg.replaceAll(">", "");
		arg = arg.replaceAll(",", "~2C");
		arg = arg.replaceAll("'", "*");
		arg = arg.replaceAll("%", "~");

		arg = arg.replaceAll("~28", "[");
		arg = arg.replaceAll("~29", "]");
		arg = arg.replaceAll("~27", "*");

		arg = arg.replaceAll("Node\\(", "");
		arg = arg.replaceAll("\\)", "]");
		arg = arg.replaceAll("\\(", "[");
		arg = arg.replaceAll("http://dbpedia.org/", "DBP#");
		arg = arg.replaceAll("http://dws/OIE", "NELL");
		return "\"" + arg.trim() + "\"";
	}
}
