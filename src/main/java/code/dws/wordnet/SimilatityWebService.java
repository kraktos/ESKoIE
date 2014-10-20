package code.dws.wordnet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import code.dws.utils.Constants;

public class SimilatityWebService {

	static String response = null;
	static double score = 0;

	static String uri = "http://swoogle.umbc.edu/SimService/GetSimilarity?operation=api";

	static HttpClient httpclient = new DefaultHttpClient();
	static HttpPost httppost = new HttpPost(uri);

	public static void main(String[] args) throws Exception {
		System.out.println(getSimScore("be in", "turn to"));

	}

	public static double getSimScore(String arg1, String arg2) throws Exception {

		try {
			// Add your data
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(2);
			nameValuePairs.add(new BasicNameValuePair("phrase1", arg1));
			nameValuePairs.add(new BasicNameValuePair("phrase2", arg2));

			httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs));

			// Execute HTTP Post Request
			HttpResponse httpResponse = httpclient.execute(httppost);

			HttpEntity httpResponseEntity = httpResponse.getEntity();

			if (httpResponseEntity != null) {
				response = EntityUtils.toString(httpResponseEntity);
			}

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if (response.trim().equals("-Infinity"))
				throw new Exception();
			score = Double.valueOf(response.trim());
		} catch (Exception e) {
			score = 0;
		}

		return score;

	}

	/**
	 * call the web service to compute the inter phrase similarity
	 * 
	 * @param properties
	 * 
	 * @param properties
	 * @param id2
	 * @param id
	 * @throws Exception
	 */
	public static void getWordNetSimilarityScores(String key1, String key2,
			BufferedWriter writerWordNet) throws Exception {

		double score = getSimScore(key1, key2);

		if (score >= 0.2)
			writerWordNet.write(key1 + "\t" + key2 + "\t"
					+ Constants.formatter.format(score) + "\n");
	}
}
