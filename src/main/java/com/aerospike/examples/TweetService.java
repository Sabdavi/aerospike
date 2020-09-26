package com.aerospike.examples;

import com.aerospike.client.*;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

import java.io.Console;

public class TweetService {
    AerospikeClient client;

    public TweetService(AerospikeClient client) {
        this.client = client;
    }
    public void createTweet() throws AerospikeException, InterruptedException {

        Console console = System.console();
        console.printf("\n********** Create Tweet **********\n");

        Record userRecord = null;
        Key userKey = null;
        Key tweetKey = null;

        // Get username
        String username;
        console.printf("\nEnter username:");
        username = console.readLine();

        if (username != null && username.length() > 0) {
            // Check if username exists
            userKey = new Key("test", "users", username);
            userRecord = client.get(null, userKey);
            if (userRecord != null) {
                int nextTweetCount = Integer.parseInt(userRecord.getValue(
                        "tweetcount").toString()) + 1;

                // Get tweet
                String tweet;
                console.printf("Enter tweet for " + username + ":");
                tweet = console.readLine();

                // Write record
                WritePolicy wPolicy = new WritePolicy();
                wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

                // Create timestamp to store along with the tweet so we can
                // query, index and report on it
                long ts = System.currentTimeMillis();

                tweetKey = new Key("test", "tweets", username + ":"
                        + nextTweetCount);
                Bin bin1 = new Bin("tweet", tweet);
                Bin bin2 = new Bin("ts", ts);
                Bin bin3 = new Bin("username", username);

                client.put(wPolicy, tweetKey, bin1, bin2, bin3);
                console.printf("\nINFO: Tweet record created!\n");

                // Update tweet count and last tweet'd timestamp in the user
                // record
                //updateUser(client, userKey, wPolicy, ts, nextTweetCount);
            } else {
                console.printf("ERROR: User record not found!\n");
            }
        }
    } //createTweet

    public void scanAllTweetsForAllUsers() {
        final Console console = System.console();
        try {
            // Java Scan
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = true;
            policy.priority = Priority.LOW;
            policy.includeBinData = true;

            client.scanAll(policy, "test", "tweets", new ScanCallback() {

                @Override
                public void scanCallback(Key key, Record record)
                        throws AerospikeException {
                    console.printf(record.getValue("tweet") + "\n");

                }
            }, "tweet");
        } catch (AerospikeException e) {
            System.out.println("EXCEPTION - Message: " + e.getMessage());
            System.out.println("EXCEPTION - StackTrace: "
                    /*+ UtilityService.printStackTrace(e)*/);
        }
    } //scanAllTweetsForAllUsers

    public void queryTweetsByUsername() throws AerospikeException {
        Console console = System.console();
        console.printf("\n********** Query Tweets By Username **********\n");

        RecordSet rs = null;
        try {
            IndexTask task = client.createIndex(null, "test", "tweets",
                    "username_index", "username", IndexType.STRING);
            task.waitTillComplete(100);

            // Get username
            String username;
            console.printf("\nEnter username:");
            username = console.readLine();

            if (username != null && username.length() > 0) {
                String[] bins = { "tweet" };
                Statement stmt = new Statement();
                stmt.setNamespace("test");
                stmt.setSetName("tweets");
                stmt.setIndexName("username_index");
                stmt.setBinNames(bins);
                stmt.setFilters(Filter.equal("username", username));

                console.printf("\nHere's " + username + "'s tweet(s):\n");

                rs = client.query(null, stmt);
                while (rs.next()) {
                    Record r = rs.getRecord();
                    console.printf(r.getValue("tweet").toString() + "\n");
                }
            } else {
                console.printf("ERROR: User record not found!\n");
            }
        } finally {
            if (rs != null) {
                // Close record set
                rs.close();
            }
        }
    } //queryTweetsByUsername

    public void queryUsersByTweetCount() throws AerospikeException {
        Console console = System.console();
        console.printf("\n********** Query Users By Tweet Count Range **********\n");

        RecordSet rs = null;
        try {
            IndexTask task = client.createIndex(null, "test", "users",
                    "tweetcount_index", "tweetcount", IndexType.NUMERIC);
            task.waitTillComplete(100);

            // Get min and max tweet counts
            int min;
            int max;
            console.printf("\nEnter Min Tweet Count:");
            min = Integer.parseInt(console.readLine());
            console.printf("Enter Max Tweet Count:");
            max = Integer.parseInt(console.readLine());

            console.printf("\nList of users with " + min + "-" + max
                    + " tweets:\n");

            String[] bins = { "username", "tweetcount", "gender" };
            Statement stmt = new Statement();
            stmt.setNamespace("test");
            stmt.setSetName("users");
            stmt.setBinNames(bins);
            stmt.setFilters(Filter.range("tweetcount", min, max));

            rs = client.query(null, stmt);
            while (rs.next()) {
                Record r = rs.getRecord();
                console.printf(r.getValue("username") + " has "
                        + r.getValue("tweetcount") + " tweets\n");
            }
        } finally {
            if (rs != null) {
                // Close record set
                rs.close();
            }
        }
    } //queryUsersByTweetCount
}
