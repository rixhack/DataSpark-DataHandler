package dh.twitter;

import static org.junit.Assert.*;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import twitter4j.Query.ResultType;
import twitter4j.Status;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TwitterDataHandlerTest extends ReceiverAdapter {
	private static ThreadLocal<TwitterDataHandler> tdh = new ThreadLocal<TwitterDataHandler>(); // = new TwitterDataHandler("");
	int margin = 5000;
	JChannel channel;
	
	@BeforeClass
	public static void setDataHandler(){
		tdh.set(new TwitterDataHandler(""));
	}
	
	@Before
	public void setChannel() throws Exception{
		channel = new JChannel();
		channel.setReceiver(this);
		channel.connect("TwitterDataCluster");
		channel.setDiscardOwnMessages(true);
	}

	@Test
	public void test1JChannel() {
		assertEquals("TwitterDataCluster", tdh.get().channel.getClusterName());
	}
	
	@Test
	public void test2Default() {
		assertEquals("", tdh.get().query);
		assertEquals(5, tdh.get().timeWindow);
		assertEquals(0, tdh.get().pValue);
		assertEquals(ResultType.recent, tdh.get().tQuery.getResultType());
		assertEquals("", tdh.get().tQuery.getQuery());
		assertEquals(100, tdh.get().tQuery.getCount());
		assertTrue(tdh.get().tweets.isEmpty());
	}
	
	@Test
	public void test3QueryAfterMessage() throws Exception {
		Object[] params = {"eindhoven", 60};
		Message msg = new Message(null, params);
		channel.send(msg);
		Thread.sleep(1000);
		assertEquals("eindhoven", tdh.get().query);
		assertEquals(60, tdh.get().timeWindow);
		Thread.sleep(3000);
		assertTrue(tdh.get().pValue > 0);
	}
	
	@Test
	public void test4TweetsContainQuery() throws InterruptedException {
		for (Status s: tdh.get().tweets){
			assertTrue(s.getText().toLowerCase().contains("eindhoven"));
		}
		Thread.sleep(10000);
		for (Status s: tdh.get().tweets){
			assertTrue(s.getText().toLowerCase().contains("eindhoven"));
		}
	}
	
	@Test
	public void test5TweetsInsideTimeWindow() throws InterruptedException{
		for (Status s: tdh.get().tweets){
			assertTrue(System.currentTimeMillis() - s.getCreatedAt().getTime() <= (tdh.get().timeWindow * 60000 + margin));
		}
		Thread.sleep(10000);
		for (Status s: tdh.get().tweets){
			assertTrue(System.currentTimeMillis() - s.getCreatedAt().getTime() <= (tdh.get().timeWindow * 60000 + margin));
		}
	}
	
	@Test
	public void test6MaxTimeWindow() throws Exception{
		Object[] params = {"#AnTUenna", 15000};
		Message msg = new Message(null, params);
		channel.send(msg);
		Thread.sleep(1000);
		assertEquals("#AnTUenna", tdh.get().query);
		assertEquals(10080, tdh.get().timeWindow);
	}
	
	@Test
	public void test7MinTimeWindow() throws Exception{
		Object[] params = {"#FelizViernes", 0};
		Message msg = new Message(null, params);
		channel.send(msg);
		Thread.sleep(1000);
		assertEquals("#FelizViernes", tdh.get().query);
		assertEquals(1, tdh.get().timeWindow);
	}

}
