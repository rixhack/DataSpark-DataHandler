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
import twitter4j.TwitterFactory;

public class TwitterDataHandler extends ReceiverAdapter {
	
	JChannel channel; // Channel for communicating with the processing library.
	private Twitter twitter; // Object to request twitter data.
	String query; // Query to send to the Twitter API.
	int pValue;	// Number of recent tweets matching the query. This value is sent to the processing sketch.
	long lastId; // The id of the newest tweet received in the last response from the Twitter API.
	Calendar fDate; // The first time, tweets created from this date will be retrieved. Default to yesterday.
	Query tQuery; // Query object
	// QueryResult qr; // The result of the query.
	List<Status> tweets; // List of returned tweets.
	int requesting = 0; // 0 if the process is not requesting data yet. 1 if it is.

	
	final int rate = 12*60; // Number of times to request new data from the Twitter API per hour. (MAX = 1800, once every two seconds).
	int timeWindow = 5; // The amount of tweets created in the last ~ minutes will be returned. (MAX = 10080 = 7 days.)
	
	// Constructor with a given query.
	public TwitterDataHandler(String query) throws Exception{
		this.query = query;
		tQuery = new Query(query);
		tQuery.setCount(100);
		tQuery.setResultType(Query.RECENT);
		twitter = TwitterFactory.getSingleton();
		twitter.getOAuth2Token(); // Request bearer token to the Twitter API.
		tweets = new ArrayList<Status>();
		start(); // Open a communication channel and wait for a message from a processing script.
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
		
		//qr = twitter.search(tQuery); // Get the tweets matching the query.
		//List<Status> newTweets = new ArrayList<Status>();
		
//		for(Status s : qr.getTweets()){
//			// If the the tweet is not a retweet and is inside the time window, add it to the new tweets.
//			if (!s.isRetweet() && System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
//				newTweets.add(s);
//			}
//		}
//		tweets.addAll(0, newTweets); // Add the new tweets to the list of current tweets.
//		if (!tweets.isEmpty()){
//			// Set the sinceId parameter to the id of the latest tweet in order to receive only newer tweets in the next query.
//			tQuery.setSinceId(tweets.get(0).getId());
//		}
		// While there are more pages, retrieve them until a tweet outside the time window is found.
		// System.out.println("Size of one page: "+tweets.size());
//		if(qr.hasNext()){
//			retrieveNextPages();
//		}
		// System.out.println("Size of all pages: "+tweets.size());
		
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
			if (!s.isRetweet() && System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
				firstPageTweets.add(s);
			} else if (!s.isRetweet()){
				done = 1;
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
					if (!s.isRetweet() && System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
						pageTweets.add(s);
					} else if (!s.isRetweet()){
						done = 1;
						break;
					}
				}
				tweets.addAll(tweets.size(), pageTweets);
			}
			if (done == 0){
				System.out.println("Current size: "+tweets.size());
				System.out.println("Current last: "+tweets.get(tweets.size()-1).getCreatedAt());
				System.out.println("Be persistent!");
				Query newQuery = new Query();
				newQuery.setQuery(query);
				newQuery.setCount(100);
				newQuery.setResultType(ResultType.recent);
				newQuery.setSince(tQuery.getSince());
				newQuery.setMaxId(tweets.get(tweets.size()-1).getId()-1);
				QueryResult qrn2 = twitter.search(newQuery);
				List<Status> nextPageTweets = new ArrayList<Status>();
				for (Status s: qrn2.getTweets()){
					// If the the tweet is not a retweet and is inside the time window, add it to the new tweets.
					if (!s.isRetweet() && System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
						nextPageTweets.add(s);
					} else if (!s.isRetweet()){
						System.out.println("Exiting2 at: https://twitter.com/statuses/" + s.getId());
						done = 1;
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
			if (!s.isRetweet() && s.getCreatedAt().getTime() > tweets.get(0).getCreatedAt().getTime()){
				firstPageTweets.add(s);
			} else if (!s.isRetweet()){
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
				System.out.println(s.getCreatedAt());
				if (!s.isRetweet() && s.getCreatedAt().getTime() > tweets.get(0).getCreatedAt().getTime()){
					pageTweets.add(s);
				} else if (!s.isRetweet()){
					System.out.println("Exiting at: https://twitter.com/statuses/" + s.getId());
					done = 1;
					break;
				}
			}
			tweets.addAll(index, pageTweets);
			index = index+pageTweets.size();
		}
	}
	
//	private void retrieveNextPages() throws Exception {
//		int exit = 0;
//		while (exit == 0){
//			while (qr.hasNext() && exit == 0){
//				Query pageQuery = qr.nextQuery();
//				qr = twitter.search(pageQuery); // Retrieve the next page of tweets.
//				List<Status> pageTweets = new ArrayList<Status>();
//				for(Status s: qr.getTweets()){
//					//System.out.println(s.getCreatedAt());
//					if (!s.isRetweet() && System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
//						pageTweets.add(s);
//					} else if (!s.isRetweet()){
//						System.out.println("Exiting at: https://twitter.com/statuses/" + s.getId());
//						exit = 1;
//						break;
//					}
//				}
//				// Add the new tweets at the end of the list (because they are older than the previous ones).
//				tweets.addAll(tweets.size(), pageTweets);
//			}
//			if (exit == 0){
//				System.out.println("Current size: "+tweets.size());
//				System.out.println("Current last: "+tweets.get(tweets.size()-1).getCreatedAt());
//				System.out.println("Be persistent!");
//				Query newQuery = new Query();
//				newQuery.setQuery(query);
//				newQuery.setCount(100);
//				newQuery.setResultType(ResultType.recent);
//				newQuery.setSince(tQuery.getSince());
//				newQuery.setMaxId(tweets.get(tweets.size()-1).getId()-1);
//				qr = twitter.search(newQuery);
//				List<Status> firstPageTweets = new ArrayList<Status>();
//				for(Status s : qr.getTweets()){
//					// If the the tweet is not a retweet and is inside the time window, add it to the new tweets.
//					if (!s.isRetweet() && System.currentTimeMillis() - s.getCreatedAt().getTime() <= timeWindow * 60000){
//						firstPageTweets.add(s);
//					} else if (!s.isRetweet()){
//						System.out.println("Exiting2 at: https://twitter.com/statuses/" + s.getId());
//						exit = 1;
//						break;
//					}
//				}
//				tweets.addAll(tweets.size(), firstPageTweets);
//			}
//		}
//	}
	
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
	
	
	private void closeChannel() {
		channel.close();
	}
}
