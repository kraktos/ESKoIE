/**
 * 
 */

package code.dws.utils;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;

/**
 * this class reads the refined output files for each of the NELL property
 * files. and stores the mappings into Database
 * 
 * @author arnab
 */
public class DBPMappingsLoader {
	// define Logger
	public static Logger logger = Logger.getLogger(DBPMappingsLoader.class
			.getName());

	static String PREDICATE;

	static THashMap<String, String> SAMEAS = new THashMap<String, String>();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });

		String clusterName = null;

		DBWrapper.init(Constants.UPDT_OIE_POSTFIXED);

		if (Constants.IS_NELL) {
			PREDICATE = args[0];
			readOutputFiles();
		} else {

			Path filePath = Paths.get("src/main/resources/output/");

			// VISIT THE FOLDER LOCATION AND ITERATE THROUGH ALL SUB FOLDERS FOR
			// THE EXACT OUTPUT FILE
			final List<Path> files = new ArrayList<Path>();
			FileVisitor<Path> fv = new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file,
						BasicFileAttributes attrs) throws IOException {
					if (file.endsWith("out.db"))
						files.add(file);
					return FileVisitResult.CONTINUE;
				}
			};

			try {
				// gets the only relevant output files
				Files.walkFileTree(filePath, fv);

				// iterate the files
				for (Path path : files) {
					clusterName = path.getParent().toString()
							.replaceAll("src/main/resources/output/", "")
							.replaceAll("ds_", "");

					PREDICATE = clusterName;

					logger.info("Currently in location " + clusterName
							+ " .... ");
					readOutputFiles();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				DBWrapper.updateResidualOIERefined();
				DBWrapper.shutDown();
			}
		}
	}

	/**
	 * reads the particular output file defined by the PREDICATE variable
	 * 
	 * @throws IOException
	 */
	private static void readOutputFiles() throws IOException {
		String path = Constants.sample_dumps + PREDICATE + "/out.db";

		@SuppressWarnings("resource")
		BufferedReader mappingsReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(path)));

		String triple;
		String value;
		String key;

		String ieSub;
		String ieProp;
		String ieObj;

		String dbpSVal;
		String dbpOVal;

		String[] arr;

		while ((triple = mappingsReader.readLine()) != null) {
			if (triple.startsWith("sameAs")) {

				arr = triple.split("\"");
				if (arr.length == 5) {

					value = arr[1].replaceAll("DBP#resource/", "");
					key = arr[3].replaceAll("NELL#Instance/", "");

					if (!SAMEAS.containsKey(key)) {
						SAMEAS.put(key, value);
					}
				}
			}
		}

		mappingsReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(path)));

		while ((triple = mappingsReader.readLine()) != null) {

			if (triple.startsWith("propAsst")) {
				arr = triple.split("\"");

				if (arr.length == 7) {

					ieProp = StringUtils.replace(arr[1], "NELL#Predicate/", "");
					ieProp = StringUtils.replace(ieProp, "_", " ");

					ieSub = StringUtils.replace(arr[3], "NELL#Instance/", "");
					ieObj = StringUtils.replace(arr[5], "NELL#Instance/", "");

					if (SAMEAS.containsKey(ieSub))
						dbpSVal = SAMEAS.get(ieSub);
					else
						dbpSVal = "X"; // dbpSVal = null;

					if (SAMEAS.containsKey(ieObj))
						dbpOVal = SAMEAS.get(ieObj);
					else
						dbpOVal = "X"; // dbpOVal = null;

					dbpOVal = (dbpOVal != null ? Utilities
							.utf8ToCharacter(dbpOVal.replaceAll("~", "%"))
							.replaceAll("\\[", "\\(").replaceAll("\\]", "\\)")
							: dbpOVal);

					dbpSVal = (dbpSVal != null ? Utilities
							.utf8ToCharacter(dbpSVal.replaceAll("~", "%"))
							.replaceAll("\\[", "\\(").replaceAll("\\]", "\\)")
							: dbpSVal);

					DBWrapper.updateOIEPostFxd(ieSub, ieProp, ieObj,
							Utilities.characterToUTF8(dbpSVal),
							Utilities.characterToUTF8(dbpOVal));
				}
			}
		}
	}

}
