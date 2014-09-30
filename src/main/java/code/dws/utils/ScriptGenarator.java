/**
 * 
 */
package code.dws.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.experiment.PropertyGoldStandard;

/**
 * @author arnab
 */
public class ScriptGenarator {

	// define class logger
	public final static Logger logger = LoggerFactory
			.getLogger(ScriptGenarator.class);

	// data separator of the NELL data file
	private static final String PATH_SEPERATOR = ",";

	static List<String> PROPS = new ArrayList<String>();

	private static final String SHELL_SCRIPT = "src/main/resources/script/PIPELINE.sh";

	private static final int MAX_BOOT_ITER = 3;

	private static final String PIPELINE_NAME = "MAPPER.sh ";

	private static final String BOOTSTRAP_NAME = "BOOTSTRAPPER.sh ";

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });
		// loadOIEProps(GenerateNewProperties.NELL_FILE_PATH);
		loadOIEProps(Constants.OIE_DATA_PATH);

		generateScript();
	}

	private static void generateScript() throws IOException {
		BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(
				SHELL_SCRIPT));

		scriptWriter.write("#!/bin/bash\n\n");

		for (String oieProp : PROPS) {

			int bootIter = 2;

			scriptWriter.write("sh ./" + PIPELINE_NAME + oieProp + "\n");
			while (bootIter != MAX_BOOT_ITER + 2) {
				scriptWriter.write("sh ./" + BOOTSTRAP_NAME + oieProp + " "
						+ bootIter++ + "\n");
			}
			scriptWriter.write("echo \"Done with complete reasoning of "
					+ oieProp + "\"\n\n");
			// System.out.println("echo \"Done with " + oieProp + "\"\n");
		}

		logger.info("echo \"Done with " + PROPS.size() + " clusters\n");
		scriptWriter.flush();
		scriptWriter.close();
	}

	/**
	 * load the valid properties in which we are interested. for Nell its easy,
	 * for Reverb this comes from the clustered ones
	 * 
	 * @param oieFilePath
	 */
	private static void loadOIEProps(String oieFilePath) {
		String oieProp = null;
		boolean flag = false;

		if (Constants.IS_NELL) {
			// // load the NELL file in memory as a collection
			// ArrayList<ArrayList<String>> nellFile =
			// FileUtil.genericFileReader(
			// GenerateNewProperties.class
			// .getResourceAsStream(oieFilePath), PATH_SEPERATOR,
			// false);
			//
			// // iterate the file
			// for (ArrayList<String> line : nellFile) {
			// oieProp = line.get(1);
			// if (!PROPS.contains(oieProp.trim()))
			// PROPS.add(oieProp);
			// }
			//
			// log.info("Loaded all properties from "
			// + GenerateNewProperties.NELL_FILE_PATH + ";  "
			// + PROPS.size());

		} else {
			if (!Constants.WORKFLOW_NORMAL) {

				// ReverbPropertyReNaming.main(new String[] { "" });
				// for (Entry<String, List<String>> e :
				// ReverbPropertyReNaming
				// .getReNamedProperties().entrySet()) {
				//
				// if (Constants.WORKFLOW_NORMAL) {
				// PROPS.add(e.getKey());
				// } else {
				//
				// // routine to selectively add only those clusters
				// // having
				// // atleast one Reverb property
				// flag = false;
				// for (String elem : e.getValue()) {
				// if (elem.indexOf(" ") != -1) {
				// flag = true;
				// }
				// }
				// if (flag)
				// PROPS.add(e.getKey());
				// else
				// System.out.println("Skipping " + e.getKey());
				// }
				// }
			} else {

				List<String> props = PropertyGoldStandard.getReverbProperties(
						oieFilePath, -1, 100L);

				// List<String> props = ReverbClusterProperty
				// .getReverbProperties(Constants.REVERB_DATA_PATH,
				// ReverbClusterProperty.TOPK_REV_PROPS);
				for (String s : props) {
					PROPS.add(s.replaceAll("\\s+", "-"));
				}
			}
		}
	}
}
