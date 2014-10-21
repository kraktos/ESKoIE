package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;

public class PairSplitter {

	// define Logger
	public static Logger logger = Logger
			.getLogger(PairSplitter.class.getName());

	private static List<String> revbProps = null;

	public static void main(String[] args) {

		Constants.loadConfigParameters(new String[] { "", args[0] });

		int cnt = 1;
		int node = 5;
		String arg1 = null;
		String arg2 = null;

		BufferedWriter pairNode1 = null;
		BufferedWriter pairNode2 = null;
		BufferedWriter pairNode3 = null;
		BufferedWriter pairNode4 = null;
		BufferedWriter pairNode5 = null;

		// only f+ properties
		DBWrapper.init(Constants.GET_FULLY_MAPPED_OIE_PROPS_SQL);
		revbProps = DBWrapper.getFullyMappedFacts();

		logger.info("Loaded " + revbProps.size() + " OIE properties");
		try {

			pairNode1 = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent() + "/pairNode1.csv"));

			pairNode2 = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent() + "/pairNode2.csv"));

			pairNode3 = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent() + "/pairNode3.csv"));

			pairNode4 = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent() + "/pairNode4.csv"));

			pairNode5 = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent() + "/pairNode5.csv"));

			for (int outerIdx = 0; outerIdx < revbProps.size(); outerIdx++) {

				arg1 = revbProps.get(outerIdx);

				for (int innerIdx = outerIdx + 1; innerIdx < revbProps.size(); innerIdx++) {

					arg2 = revbProps.get(innerIdx);

					if (cnt % node == 1) {
						pairNode1.write(arg1 + "\t" + arg2 + "\n");
					} else if (cnt % node == 2) {
						pairNode2.write(arg1 + "\t" + arg2 + "\n");
					} else if (cnt % node == 3) {
						pairNode3.write(arg1 + "\t" + arg2 + "\n");
					} else if (cnt % node == 4) {
						pairNode4.write(arg1 + "\t" + arg2 + "\n");
					} else if (cnt % node == 0) {
						pairNode5.write(arg1 + "\t" + arg2 + "\n");
					}
					cnt++;
				}

				pairNode1.flush();
				pairNode2.flush();
				pairNode3.flush();
				pairNode4.flush();
				pairNode5.flush();
			}

		} catch (IOException e) {

		} finally {
			try {

				pairNode1.close();

				pairNode2.close();
				pairNode3.close();

				pairNode4.close();
				pairNode5.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}
}
