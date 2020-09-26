package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;

import java.io.Console;

public class Program {
    private final String SERVER ;
    private final int PORT ;

    private AerospikeClient client;
    public Program(String server,int port) {
        this.SERVER = server;
        this.PORT = port;
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.timeout = 500;
        client = new AerospikeClient(clientPolicy, SERVER, PORT);
    }

    @Override
    protected void finalize() throws Throwable{
        if(client !=null)
            client.close();
    }

    public void work(){
        Console console = System.console();
        try {
            console.printf("INFO: Connecting to Aerospike cluster...");

            // Establish connection to Aerospike server

            if (client == null || !client.isConnected()) {
                System.out.printf("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!");
                console.readLine();
            } else {
                console.printf("\nINFO: Connection to Aerospike cluster succeeded!\n");

                // Create instance of UserService
                UserService us = new UserService(client);
                // Create instance of TweetService
                TweetService ts = new TweetService(client);
                // Create instance of UtilityService
                // UtilityService util = new UtilityService(client);

                // Present options
                console.printf("\nWhat would you like to do:\n");
                console.printf("1> Create A User And A Tweet\n");
                console.printf("2> Read A User Record\n");
                console.printf("3> Batch Read Tweets For A User\n");
                console.printf("4> Scan All Tweets For All Users\n");
                console.printf("5> Record UDF -- Update User Password\n");
                console.printf("6> Query Tweets By Username And Users By Tweet Count Range\n");
                console.printf("7> Stream UDF -- Aggregation Based on Tweet Count By Region\n");
                console.printf("0> Exit\n");
                console.printf("\nSelect 0-7 and hit enter:\n");
                int feature = Integer.parseInt(console.readLine());

                if (feature != 0) {
                    switch (feature) {
                        case 1:
                            console.printf("\n********** Your Selection: Create User And A Tweet **********\n");
                                us.createUser();
                                ts.createTweet();
                            break;
                        case 2:
                            console.printf("\n********** Your Selection: Read A User Record **********\n");
                                us.getUser();
                            break;
                        case 3:
                            console.printf("\n********** Your Selection: Batch Read Tweets For A User **********\n");

                            us.batchGetUserTweets();
                            break;
                        case 4:
                            console.printf("\n********** Your Selection: Scan All Tweets For All Users **********\n");
                            ts.scanAllTweetsForAllUsers();
                                break;
                        case 5:
                            console.printf("\n********** Your Selection: Record UDF -- Update User Password **********\n");
                            us.updatePasswordUsingUDF();
                            break;
                        case 6:
                            console.printf("\n********** Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n");
                             ts.queryTweetsByUsername();
                             ts.queryUsersByTweetCount();
                            break;
                        case 7:
                            console.printf("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n");
                            // us.aggregateUsersByTweetCountByRegion();
                            break;
                        case 12:
                            console.printf("\n********** Create Users **********\n");
                            // us.createUsers();
                            break;
                        case 23:
                            console.printf("\n********** Create Tweets **********\n");
                            // ts.createTweets();
                            break;
                        default:
                            break;
                    }
                }
            }
        } catch (AerospikeException e) {
            console.printf("AerospikeException - Message: " + e.getMessage()
                    + "\n");
            console.printf("AerospikeException - StackTrace");
        } catch (Exception e) {
            console.printf("Exception - Message: " + e.getMessage() + "\n");
            console.printf("Exception - StackTrace: " + "\n");
        } finally {
            if (client != null && client.isConnected()) {
                // Close Aerospike server connection
                client.close();
            }
            console.printf("\n\nINFO: Press any key to exit...\n");
            console.readLine();
        }
    }
}
