package code.dws.utils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to perform IO operations
 * 
 * @author Arnab Dutta
 */
public class FileUtil {

	public final static Logger logger = LoggerFactory.getLogger(FileUtil.class);

	public static ArrayList<ArrayList<String>> genericFileReader(
			InputStream inputStream, String valueSeperator, boolean hasHeader) {

		Scanner scan;
		scan = new Scanner(inputStream, "UTF-8");

		if (hasHeader) {
			scan.nextLine();
		}
		ArrayList<ArrayList<String>> lines = new ArrayList<ArrayList<String>>();

		while (scan.hasNextLine()) {

			ArrayList<String> tokens = new ArrayList<String>();

			String line = scan.nextLine();
			StringTokenizer st = new StringTokenizer(line, valueSeperator);

			while (st.hasMoreTokens()) {
				tokens.add(st.nextToken());
			}
			lines.add(tokens);
		}

		scan.close();

		return lines;
	}



	


}
