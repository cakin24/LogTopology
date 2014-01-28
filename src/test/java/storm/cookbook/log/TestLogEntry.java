package storm.cookbook.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import storm.cookbook.log.model.LogEntry;

public class TestLogEntry extends StormTestCase{

	@Test
	public void testFromJSON() throws IOException, Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date test = format.parse("2013-01-04T11:09:16.171");
		String testData = UnitTestUtils.readFile("/testData1.json");
		JSONObject obj=(JSONObject) JSONValue.parse(testData);
		LogEntry entry = new LogEntry(obj);
		assertEquals("file://logServer/var/log/auth.log", entry.getSource());
		assertEquals(0,entry.getTags().size());
		assertEquals(0,entry.getFields().size());
		assertEquals(test, entry.getTimestamp());
		assertEquals("logServer",entry.getSourceHost());
		assertEquals("/var/log/auth.log",entry.getSourcePath());
		assertNotNull(entry.getMessage());
		assertEquals("syslog", entry.getType());
	}
	
	@Test
	public void testToJSON() throws IOException{
		String testData = UnitTestUtils.readFile("/testData1.json");
		JSONObject obj=(JSONObject) JSONValue.parse(testData);
		LogEntry entry = new LogEntry(obj);
		JSONObject test = entry.toJSON();
		assertEquals(obj.get("@source"), test.get("@source"));
		assertEquals(obj.get("@source_host"), test.get("@source_host"));
		assertEquals(obj.get("@source_path"), test.get("@source_path"));
		assertEquals(obj.get("@message"), test.get("@message"));
		assertEquals(obj.get("@type"), test.get("@type"));
		assertEquals(obj.get("@source"), test.get("@source"));
		assertEquals(DateFormat.getDateInstance().format(entry.getTimestamp()), test.get("@timestamp"));
	}
	
	@Test
	public void testEquals() throws IOException{
		LogEntry lhs = getEntry();
		LogEntry rhs = getEntry();
		assertEquals(lhs, rhs);
	}

}
