package com.aerospike.examples;

import com.aerospike.client.*;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;

import java.io.Console;
import java.io.File;
import java.util.Arrays;

public class UserService {
    private AerospikeClient client;

    public UserService(AerospikeClient client) {
        this.client = client;
    }
    public void createUser() throws AerospikeException {
        Console console = System.console();

        console.printf("\n********** Create User **********\n");

        String username;
        String password;
        String gender;
        String region;
        String interests;

        // Get username
        console.printf("Enter username: ");
        username = console.readLine();

        if (username != null && username.length() > 0) {
            // Get password
            console.printf("Enter password for " + username + ":");
            password = console.readLine();

            // Get gender
            console.printf("Select gender (f or m) for " + username + ":");
            gender = console.readLine().substring(0, 1);

            // Get region
            console.printf("Select region (north, south, east or west) for "
                    + username + ":");
            region = console.readLine().substring(0, 1);

            // Get interests
            console.printf("Enter comma-separated interests for " + username + ":");
            interests = console.readLine();

            // Write record
            WritePolicy wPolicy = new WritePolicy();
            wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

            Key key = new Key("test", "users", username);
            Bin bin1 = new Bin("username", username);
            Bin bin2 = new Bin("password", password);
            Bin bin3 = new Bin("gender", gender);
            Bin bin4 = new Bin("region", region);
            Bin bin5 = new Bin("lasttweeted", 0);
            Bin bin6 = new Bin("tweetcount", 0);
            Bin bin7 = Bin.asList("interests", Arrays.asList(interests.split(",")));

            client.put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

            console.printf("\nINFO: User record created!");
        }
    }
    public void getUser() throws AerospikeException {
        Console console = System.console();
        Record userRecord = null;
        Key userKey = null;

        // Get username
        String username;
        console.printf("\nEnter username:");
        username = console.readLine();

        if (username != null && username.length() > 0) {
            // Check if username exists
            userKey = new Key("test", "users", username);
            userRecord = client.get(null, userKey);
            if (userRecord != null) {
                console.printf("\nINFO: User record read successfully! Here are the details:\n");
                console.printf("username:   " + userRecord.getValue("username")
                        + "\n");
                console.printf("password:   " + userRecord.getValue("password")
                        + "\n");
                console.printf("gender:     " + userRecord.getValue("gender") + "\n");
                console.printf("region:     " + userRecord.getValue("region") + "\n");
                console.printf("tweetcount: " + userRecord.getValue("tweetcount") + "\n");
                console.printf("interests:  " + userRecord.getValue("interests") + "\n");
            } else {
                console.printf("ERROR: User record not found!\n");
            }
        } else {
            console.printf("ERROR: User record not found!\n");
        }
    } //getUser

    public void batchGetUserTweets() throws AerospikeException {
        Console console = System.console();
        Record userRecord = null;
        Key userKey = null;

        // Get username
        String username;
        console.printf("\nEnter username:");
        username = console.readLine();

        if (username != null && username.length() > 0) {
            // Check if username exists
            userKey = new Key("test", "users", username);
            userRecord = client.get(null, userKey);
            if (userRecord != null) {
                // Get how many tweets the user has
                int tweetCount = (Integer) userRecord.getValue("tweetcount");

                // Create an array of keys so we can initiate batch read
                // operation
                Key[] keys = new Key[tweetCount];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = new Key("test", "tweets",
                            (username + ":" + (i + 1)));
                }

                console.printf("\nHere's " + username + "'s tweet(s):\n");

                // Initiate batch read operation
                if (keys.length > 0){
                    Record[] records = client.get(new Policy(), keys);
                    for (int j = 0; j < records.length; j++) {
                        console.printf(records[j].getValue("tweet").toString() + "\n");
                    }
                }
            }
        } else {
            console.printf("ERROR: User record not found!\n");
        }
    } //batchGetUserTweets

    public void updatePasswordUsingUDF() throws AerospikeException
    {
        Console console = System.console();
        Record userRecord = null;
        Key userKey = null;

        // Get username
        String username;
        console.printf("\nEnter username:");
        username = console.readLine();

        if (username != null && username.length() > 0)
        {
            // Check if username exists
            userKey = new Key("test", "users", username);
            userRecord = client.get(null, userKey);
            if (userRecord != null)
            {
                // Get new password
                String password;
                console.printf("Enter new password for " + username + ":");
                password = console.readLine();
                LuaConfig.SourceDirectory = "udf";
                File udfFile = new File("udf/updateUserPwd.lua");

                RegisterTask rt = client.register(null, udfFile.getPath(),
                        udfFile.getName(), Language.LUA);
                rt.waitTillComplete(100);

                String updatedPassword = client.execute(null, userKey, "updateUserPwd", "updatePassword", Value.get(password)).toString();
                console.printf("\nINFO: The password has been set to: " + updatedPassword);
            }
            else
            {
                console.printf("ERROR: User record not found!");
            }
        }
        else
        {
            console.printf("ERROR: User record not found!");
        }
    } //updatePasswordUsingUDF
}
