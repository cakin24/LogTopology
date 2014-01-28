package storm.cookbook.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import storm.cookbook.log.model.LogEntry;

public class UnitTestUtils {
	
	public static String readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				IntegrationTestTopology.class.getResourceAsStream(file)));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}

		return stringBuilder.toString();
	}
	
	public static LogEntry getEntry() throws IOException{
		String testData = UnitTestUtils.readFile("/testData1.json");
		JSONObject obj=(JSONObject) JSONValue.parse(testData);
		return new LogEntry(obj);
	}

}
