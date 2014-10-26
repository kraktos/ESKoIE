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
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import code.dws.utils.Constants;

public class SimilatityWebService {

	// define Logger
	public static Logger logger = Logger.getLogger(SimilatityWebService.class
			.getName());

	static String response = null;
	static double score = 0;

	static String uri = "http://swoogle.umbc.edu/SimService/GetSimilarity?operation=api";
	static CloseableHttpClient httpclient = null;

	public static void main(String[] args) throws Exception {
		System.out
				.println(getSimScore("also appeared in", "did not respond to"));

	}

	public static void init() {
		PoolingHttpClientConnectionManager poolManager = new PoolingHttpClientConnectionManager();
		poolManager.setMaxTotal(Constants.HTTP_CONN_MAX_TOTAL);
		poolManager.setDefaultMaxPerRoute(Constants.HTTP_CONN_MAX_TOTAL_PER_ROUTE);

		httpclient = HttpClients.custom().setConnectionManager(poolManager)
				.build();
	}

	public static void closeDown() {
		try {
			httpclient.close();
		} catch (IOException e) {
			logger.error("");
		}
	}

	public static double getSimScore(String arg1, String arg2) throws Exception {

		List<NameValuePair> nameValuePairs = null;

		try {
			// Add your data
			nameValuePairs = new ArrayList<NameValuePair>(2);
			nameValuePairs.add(new BasicNameValuePair("phrase1", arg1));
			nameValuePairs.add(new BasicNameValuePair("phrase2", arg2));

			// HttpClient httpclient = HttpClientBuilder.create().build();

			HttpPost httppost = new HttpPost(uri);

			httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs));

			// Execute HTTP Post Request
			HttpResponse httpResponse = httpclient.execute(httppost);

			HttpEntity httpResponseEntity = httpResponse.getEntity();

			if (httpResponseEntity != null) {
				response = EntityUtils.toString(httpResponseEntity);
			}

		} catch (ConnectTimeoutException cTo) {
			logger.error("ConnectTimeoutException out with " + arg1 + ", "
					+ arg2);
		} catch (ClientProtocolException e) {
			logger.error("ClientProtocolException with " + arg1 + ", " + arg2);
		} catch (IOException e) {
			logger.error("IOException  with " + arg1 + ", " + arg2);
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
