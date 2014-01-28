package storm.cookbook.log;

import java.io.IOException;

import org.jmock.Expectations;
import org.junit.Test;

import storm.cookbook.log.model.LogEntry;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestVolumeCountingBolt extends StormTestCase {

	@Test
	public void test() throws IOException {
		final OutputCollector collector = context.mock(OutputCollector.class);
        final LogEntry entry = getEntry();
        final Tuple tuple = getTuple();

        VolumeCountingBolt bolt = new VolumeCountingBolt();
        bolt.prepare(null, null, collector);
        
        final long expectedMilliseonds = bolt.getMinuteForTime(entry.getTimestamp());
        
        context.checking(new Expectations(){{
        	oneOf(tuple).getValueByField(FieldNames.LOG_ENTRY);will(returnValue(entry));
        	oneOf(collector).emit(new Values(expectedMilliseonds, entry.getSource(), 1L));
        }});
        
        bolt.execute(tuple);
        context.assertIsSatisfied();
	}

}
