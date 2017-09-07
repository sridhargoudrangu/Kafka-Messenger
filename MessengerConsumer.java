// import java packages
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

// import HttpClient packages
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

// import Apache Kafka packages
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

// import JSON packages
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MessengerConsumer implements Runnable {
    private String topicName;
    private String groupID;
    private String destinationURL;
    private KafkaConsumer<String, String> consumer;
    private String Authtoken;

    // constructor for each individual thread
    public MessengerConsumer(String topicName, String groupID, String destinationURL, String broker, String Authtoken) {
        this.topicName = topicName;
        this.groupID = groupID;
        this.destinationURL = destinationURL;
        Properties props = propConfig(groupID, broker);
        // create new Apache Kafka Consumer
        this.consumer = new KafkaConsumer<String, String>(props);
        this.Authtoken = Authtoken;
    }

    public static Properties propConfig(String groupID, String broker) {
        // set properties and groupID
        Properties props = new Properties();
        try {
            props.put("client.id", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        props.put("group.id", "foo");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // props.put("auto.offset.reset", "earliest");
        // props.put("client.id.config", "simple");
        props.put("format","json");
        // props.put("name", "my_consumer_instance");
        return props;
    }

    // returns consumer
    public KafkaConsumer<String, String> getKafkaConsumer() {
        return this.consumer;
    }

    @Override
    public void run() {
        // Kafka Consumer subscribes list of topics
        consumer.subscribe(Arrays.asList(this.topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                // create JSONObjects amd fill JSONArray
                for (ConsumerRecord<String, String> record : records) {
                    JSONParser parser = new JSONParser();
                    JSONObject currentObject = (JSONObject) parser.parse(record.value());
                    JSONArray array = new JSONArray();
                    array.add(currentObject);
                    System.out.println(array.toJSONString());
                    postArray(array);
                }

                if (records.count() > 0) {
                    break;
                }

                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    e.printStackTrace();
                }
            }
        }
        catch(ParseException ex) {
            System.out.println("Exception caught " + ex.getMessage());
        }
        finally {
            // consumer.close();
            System.out.println("Consumer closed.");
        }
    }

    // posts JSON to the DataCube
    public void post(JSONObject json) {
        String url = this.destinationURL;
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);

        StringEntity entity = null;
        try {
            entity = new StringEntity(json.toJSONString());
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        post.setEntity(entity);
        post.addHeader("Authtoken", this.Authtoken);
        post.addHeader("Accept", "application/json");
        post.addHeader("Content-type", "application/json");

        HttpResponse response = null;
        try {
            response = client.execute(post);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response.getStatusLine());
    }

    // posts JSONArray to the DataCube
    public void postArray(JSONArray array) {
        String url = destinationURL;
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);

        StringEntity entity = null;
        try {
            entity = new StringEntity(array.toString());
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        post.setEntity(entity);
        post.addHeader("Authtoken", this.Authtoken);
        post.addHeader("Accept", "application/json");
        post.addHeader("Content-type", "application/json");

        HttpResponse response = null;
        try {
            response = client.execute(post);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response.getStatusLine());
    }

    public static void main(String args[]) throws IOException, ParseException {
    }
}

