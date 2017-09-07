import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class MessengerProducer {
    private String broker;
    private String topic; // the name of the topic and the name of the input file are the same
    private String destinationURL;
    private String Authtoken;

    public MessengerProducer(String topic) throws IOException, ParseException {
        this.broker = "localhost:8082";
        this.topic = topic;
        this.destinationURL = destinationURL;
        this.Authtoken = Authtoken;

        int numberOfPartitions = partitionCalculator(topic);
        // createTopic(topic, numberOfPartitions, 1);
        produce(topic);
    }

    // returns the length of the file
    private static int partitionCalculator(String fileName) {
        int partitionCount = 1;
        try {
            JSONParser parser = new JSONParser();
            JSONArray array = (JSONArray) parser.parse(new FileReader(fileName));
            if (array.size() % 10 != 0) {
                partitionCount = array.size() / 10 + 1;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return partitionCount;
    }

    public void produce(String topic) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray array = (JSONArray) parser.parse(new FileReader(topic));
            for (Object object : array) {
                JSONObject current = (JSONObject) object;
                System.out.println("Object: " + current.toJSONString());
                String url = "http://localhost:8082/topics/" + topic;
                // System.out.println(url);
                HttpClient client = HttpClientBuilder.create().build();
                HttpPost post = new HttpPost(url);

                String body = "{\"records\":[{\"value\":" + current.toJSONString() + "}]}";
                StringEntity entity = new StringEntity(body);
                post.setEntity(entity);

                post.addHeader("Accept", "application/vnd.kafka.v2+json");
                post.addHeader("Content-type", "application/vnd.kafka.json.v2+json");

                HttpResponse response = null;
                try {
                    response = client.execute(post);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }

                String statusLine = response.getStatusLine().toString();
                System.out.println("Status: " + statusLine);
                String responseString = new BasicResponseHandler().handleResponse(response);
                System.out.println("Response: " + responseString + "\n");

            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // creates topic given topicName, number of partitions, and replication factor
    public void createTopic(String topicName, int numberOfPartitions, int replicationFactor) {
        try {
            String command = "./bin/kafka-topics --zookeeper " + this.broker + " --create --topic " + topicName + " --partitions " + numberOfPartitions + " --replication-factor " + replicationFactor;
            System.out.println(command);
            Runtime rt = Runtime.getRuntime();
            //Process pr =  rt.exec(command,new String[]{}, new File(""));
            Process pr =  rt.exec(command);
        }
        catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    // returns list of all topics
    public void listOfTopics() {
        String url = "http://localhost:8082/topics/";
        System.out.println(url);
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet get = new HttpGet(url);

        HttpResponse response = null;
        try {
            response = client.execute(get);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        String statusLine = response.getStatusLine().toString();
        System.out.println("Status: " + statusLine);
        try {
            String responseString = new BasicResponseHandler().handleResponse(response);
            System.out.println("Response: " + responseString + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // returns information aobut a topic, including number of partitions and leader
    public void topicInfo(String topic) {
        String url = "http://localhost:8082/topics/" + topic;
        System.out.println(url);
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet get = new HttpGet(url);

        HttpResponse response = null;
        try {
            response = client.execute(get);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        String statusLine = response.getStatusLine().toString();
        System.out.println("Status: " + statusLine);
        try {
            String responseString = new BasicResponseHandler().handleResponse(response);
            System.out.println("Response: " + responseString + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // returns information about a topic's partitions
    public void partitionInfo(String topic) {
        String url = "http://localhost:8082/topics/" + topic + "/partitions";
        System.out.println(url);
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet get = new HttpGet(url);

        HttpResponse response = null;
        try {
            response = client.execute(get);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        String statusLine = response.getStatusLine().toString();
        System.out.println("Status: " + statusLine);
        try {
            String responseString = new BasicResponseHandler().handleResponse(response);
            System.out.println("Response: " + responseString + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        Logger.getRootLogger().setLevel(Level.OFF);
        String topic = "jsontest4.json";

        MessengerProducer producer = new MessengerProducer(topic);
        producer.listOfTopics();
        producer.topicInfo(topic);
        producer.partitionInfo(topic);
    }
}