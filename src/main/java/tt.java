import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

public class tt {
	// define Logger
	public static Logger logger = Logger.getLogger(tt.class.getName());

	public static void main(String[] args) {
		Scanner scan = null;
		String line = null;
		String[] arr = null;

		int total = 0;
		int rvb = 0;
		int dbpProps = 0;

		Map<String, Long> sss = new HashMap<String, Long>();

		try {
			scan = new Scanner(
					new File(
							"/home/adutta/git/OIE-Integration/src/main/resources/input/cluster.names.original.out"),
					"UTF-8");
			logger.info("Reading file "
					+ "/home/adutta/git/OIE-Integration/src/main/resources/input/cluster.names.original.out");

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.trim().split("\t");

				boolean flag = false;
				total = 0;
				for (String elem : arr) {
					if (elem.indexOf("C") == -1) {
						elem = elem.replaceAll("\\[", "");
						elem = elem.replaceAll("\\]", "");
						if (elem.length() > 0) {
							if (elem.indexOf(" ") == -1) {
								flag = true;
							} else
								total++;
						}
					}
				}

				if (!flag)
					rvb = rvb + total;
			}

			logger.info("rvb = " + rvb);

		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		}

	}
}
