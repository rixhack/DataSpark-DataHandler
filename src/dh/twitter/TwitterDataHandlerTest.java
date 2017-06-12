package dh.twitter;

import static org.junit.Assert.*;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.junit.Before;
import org.junit.Test;

import twitter4j.Query.ResultType;
import twitter4j.Status;

public class TwitterDataHandlerTest extends ReceiverAdapter {
	TwitterDataHandler tdh = new TwitterDataHandler("");
	int margin = 5000;
	JChannel channel;
	
	@Before
	public void setChannel() throws Exception{
		channel = new JChannel();
		channel.setReceiver(this);
		channel.connect("TwitterDataCluster");
		channel.setDiscardOwnMessages(true);
	}

	
	@Test
	public void testDefault() {
		assertEquals("", tdh.query);
		assertEquals(5, tdh.timeWindow);
		assertEquals(0, tdh.pValue);
		assertEquals(ResultType.recent, tdh.tQuery.getResultType());
		assertEquals("", tdh.tQuery.getQuery());
		assertEquals(100, tdh.tQuery.getCount());
		assertTrue(tdh.tweets.isEmpty());
	}
	
	@Test
	public void testJChannel() {
		assertEquals("TwitterDataCluster", tdh.channel.getClusterName());
	}
	
	@Test
	public void testQueryAfterMessage() throws Exception {
		Object[] params = {"eindhoven", 60};
		Message msg = new Message(null, params);
		channel.send(msg);
		Thread.sleep(1000);
		assertEquals("eindhoven", tdh.query);
		assertEquals(60, tdh.timeWindow);
		Thread.sleep(3000);
		assertTrue(tdh.pValue > 0);
	}
	
	@Test
	public void testTweetsContainQuery() throws InterruptedException{
		for (Status s: tdh.tweets){
			assertTrue(s.getText().contains("eindhoven"));
		}
		Thread.sleep(10000);
		for (Status s: tdh.tweets){
			assertTrue(s.getText().contains("eindhoven"));
		}
	}
	
	@Test
	public void testTweetsInsideTimeWindow() throws InterruptedException{
		for (Status s: tdh.tweets){
			assertTrue(System.currentTimeMillis() - s.getCreatedAt().getTime() <= (tdh.timeWindow+margin));
		}
		Thread.sleep(10000);
		for (Status s: tdh.tweets){
			assertTrue(System.currentTimeMillis() - s.getCreatedAt().getTime() <= (tdh.timeWindow+margin));
		}
	}
	
	@Test
	public void testMaxTimeWindow() throws Exception{
		Object[] params = {"#AnTUenna", 15000};
		Message msg = new Message(null, params);
		channel.send(msg);
		Thread.sleep(1000);
		assertEquals("#AnTUenna", tdh.query);
		assertEquals(10080, tdh.timeWindow);
	}
	
	@Test
	public void testMinTimeWindow() throws Exception{
		Object[] params = {"#FelizViernes", 0};
		Message msg = new Message(null, params);
		channel.send(msg);
		Thread.sleep(1000);
		assertEquals("#FelizViernes", tdh.query);
		assertEquals(1, tdh.timeWindow);
	}

}
