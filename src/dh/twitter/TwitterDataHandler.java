package dh.twitter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import twitter4j.Query;
import twitter4j.Query.ResultType;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TwitterDataHandler extends ReceiverAdapter {
	
	JChannel channel; // Channel for communicating with the processing library.
	private Twitter twitter; // Object to request twitter data.
	String query; // Query to send to the Twitter API.
	int pValue = 0;	// Number of recent tweets matching the query. This value is sent to the processing sketch.
	long lastId; // The id of the newest tweet received in the last response from the Twitter API.
	Calendar fDate; // The first time, tweets created from this date will be retrieved. Default to yesterday.
	Query tQuery; // Query object
	// QueryResult qr; // The result of the query.
	List<Status> tweets; // List of returned tweets.
	int requesting = 0; // 0 if the process is not requesting data yet. 1 if it is.

	
	final int rate = 12*60; // Number of times to request new data from the Twitter API per hour. (MAX = 1800, once every two seconds).
	int timeWindow = 5; // The amount of tweets created in the last ~ minutes will be returned. (MAX = 10080 = 7 days.)
	
	// Constructor with a given query.
	public TwitterDataHandler(String query) {
		this.query = query;
		tQuery = new Query(query);
		tQuery.setCount(100);
		tQuery.setResultType(Query.RECENT);
		twitter = TwitterFactory.getSingleton();
		try {
			twitter.getOAuth2Token();
		} catch (Exception e) {
			// e.printStackTrace(); // Request bearer token to the Twitter API.
		} 
		tweets = new ArrayList<Status>();
		try {
			start(); // Open a communication channel and wait for a message from a processing script.
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public static void main(String[] args) throws Exception{
		try {
			// Initialise with an empty query and wait for a message from a processing sketch.
			new TwitterDataHandler("");
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	// Create the channel and join the Twitter cluster.
	private void start() throws Exception {
		channel = new JChannel();
		channel.setReceiver(this);
		channel.connect("TwitterDataCluster");
		channel.setDiscardOwnMessages(true); // Avoids the data handler to receive its own messages.
	}
	
	// When a message is received, set the query and start requesting data.
	public void receive(Message msg){
		try {
			Object[] array = (Object[]) msg.getObject();
			this.query = (String) array[0];
			tweets.clear();
			tQuery.setSinceId(0);
			int tw = (int) array[1];
			if (tw < 1){
				this.timeWindow = 1;
			} else if (tw > 10080){
				this.timeWindow = 10080;
			} else {
				this.timeWindow = (int) array[1];
			}
			setDate();
			tQuery.setQuery(query);
			if (requesting == 0){
				try {
					startRequesting();
					requesting = 1;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (ClassCastException e){
			
		}
	}
	
	// Create a separate thread to request data with a frequency defined by the rate variable.
	public void startRequesting(){
		new Thread(new Runnable() {
			public void run(){
				while (true) {
					try {
						newData();
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					try {
						Thread.sleep(3600/rate*1000);
					} catch (InterruptedException e){
						
					}
				}
			}
		}).start();
	}
	
	private void newData() throws Exception {		
		pValue = getData();
		Message msg = new Message(null, pValue);
		channel.send(msg); // Send the value to the processing sketch.
	}
	
	private int getData() throws Exception {
		
		System.out.println("Since: "+ tQuery.getSince());
		System.out.println("Query: "+ tQuery.getQuery());
		System.out.println("Time window: " + timeWindow);
		
		if (tweets.isEmpty()){
			getAllTweets();
		} else {
			getNewTweets();
		}
		
		deleteOldTweets(); // Delete tweets outside the time window.
		
		if (!tweets.isEmpty()){
			System.out.println("First one: https://twitter.com/statuses/" + tweets.get(0).getId());
			System.out.println("Last one: https://twitter.com/statuses/" + tweets.get(tweets.size()-1).getId());
		}
		System.out.println("Size: "+tweets.size());
		return tweets.size();
	}
	
	private void getAllTweets() throws Exception {
		int done = 0;
		QueryResult qra = twitter.search(tQuery);
		List<Status> firstPageTweets = new ArrayList<Status>();
		for (Status s: qra.getTweets()){
			done = checkTweet(s, firstPageTweets);
			if (done == 1){
				break;
			}
		}
		tweets.addAll(tweets.size(), firstPageTweets);
		while (done == 0){
			while (qra.hasNext() && done == 0){
				Query pageQuery = qra.nextQuery();
				qra = twitter.search(pageQuery);
				List<Status> pageTweets = new ArrayList<Status>();
				for (Status s: qra.getTweets()){
					done = checkTweet(s, pageTweets);
					if (done == 1){
						break;
					}
				}
				tweets.addAll(tweets.size(), pageTweets);
			}
			if (done == 0 && !tweets.isEmpty()){
				System.out.println("Current size: "+tweets.size());
				System.out.println("Current last: "+tweets.get(tweets.size()-1).getCreatedAt());
				System.out.println("Be persistent!");
				Query newQuery = new Query();
				newQuery.setQuery(query);
				newQuery.setCount(100);
				newQuery.setResultType(ResultType.recent);
				newQuery.setSince(tQuery.getSince());
				if(!tweets.isEmpty()){
					newQuery.setMaxId(tweets.get(tweets.size()-1).getId()-1);
				}
				QueryResult qrn2 = twitter.search(newQuery);
				List<Status> nextPageTweets = new ArrayList<Status>();
				for (Status s: qrn2.getTweets()){
					done = checkTweet(s, nextPageTweets);
					if (done == 1){
						break;
					}
				}
				tweets.addAll(tweets.size(), nextPageTweets);
			}
		}
	}
	
	private void getNewTweets() throws Exception {
		int done = 0;
		tQuery.setSinceId(tweets.get(0).getId());
		System.out.println("Getting tweets newer than: "+tweets.get(0).getCreatedAt());
		QueryResult qrn = twitter.search(tQuery);
		List<Status> firstPageTweets = new ArrayList<Status>();
		for (Status s: qrn.getTweets()){
			if (s.getCreatedAt().getTime() > tweets.get(0).getCreatedAt().getTime()){
				if (!s.isRetweet() && s.getText().toLowerCase().contains(query.toLowerCase())){
					firstPageTweets.add(s);
				}
			} else {
				done = 1;
				break;
			}
		}
		tweets.addAll(0,firstPageTweets);
		int index = firstPageTweets.size();
		while (qrn.hasNext() && done == 0){
			Query pageQuery = qrn.nextQuery();
			qrn = twitter.search(pageQuery);
			List<Status> pageTweets = new ArrayList<Status>();
			for (Status s: qrn.getTweets()){
				if (s.getCreatedAt().getTime() > tweets.get(0).getCreatedAt().getTime()){
					if (!s.isRetweet() && s.getText().toLowerCase().contains(query.toLowerCase())){
						firstPageTweets.add(s);
					}
				} else {
					done = 1;
					break;
				}
			}
			tweets.addAll(index, pageTweets);
			index = index+pageTweets.size();
		}
	}
	
	private void deleteOldTweets() {
		for (int i = tweets.size()-1; i >= 0; i--) {
			Date createdAt = tweets.get(i).getCreatedAt();
			if (System.currentTimeMillis() - createdAt.getTime() > timeWindow * 60000){
				System.out.println("Deleted: https://twitter.com/statuses/" + tweets.get(i).getId());
				tweets.remove(i);
			} else {
				break;
			}
		}
	}
	
	private void setDate() {
		int days = timeWindow/1440+1;
		fDate = Calendar.getInstance(); 
		fDate.add(Calendar.DATE, -days);
		int year = fDate.get(Calendar.YEAR);
		int month = fDate.get(Calendar.MONTH)+1;
		int dayOfMonth = fDate.get(Calendar.DAY_OF_MONTH);
		tQuery.setSince(year + "-" + month + "-" + dayOfMonth);
	}
	
	private int checkTweet(Status s, List<Status> list){
		if (System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
			if (!s.isRetweet() && s.getText().toLowerCase().contains(query.toLowerCase())){
				list.add(s);
			}
		} else {
			return 1;
		}
		return 0;
	}
	
	private void closeChannel() {
		channel.close();
	}
}
