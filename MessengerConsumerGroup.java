// import java packages
import java.util.*;

// create consumer group (multiple threads of consumer clients)
public class MessengerConsumerGroup {
    private String broker; // address for Kafka server
    private String topicName; // the name of the topic and the name of the input file are the same
    private String groupID; // name of consumer group
    private int numberOfConsumers; // number of consumers to be created
    private String destinationURL; // name of destination URL
    private String Authtoken; // authentication token for access to Commvault DataCube
    private List<MessengerConsumer> consumers; // list holding multiple threads ofconsumers

    // initialize instance variables for consumer group
    public MessengerConsumerGroup(String broker, String groupID, String topicName, int numberOfConsumers, String destinationURL, String Authtoken) {
        this.broker = broker; //
        this.groupID = groupID;
        this.topicName = topicName;
        this.numberOfConsumers = numberOfConsumers;
        this.destinationURL = destinationURL;
        this.Authtoken = Authtoken;
        this.consumers = new ArrayList<>();
        for (int i = 0; i < this.numberOfConsumers; i++) {
            MessengerConsumer consumerThread = new MessengerConsumer(this.topicName, this.groupID, this.destinationURL, this.broker, this.Authtoken);
            this.consumers.add(consumerThread);
        }
    }

    // run the consumer group
    public void runGroup() {
        for (MessengerConsumer thread : consumers) {
            Thread current = new Thread(thread);
            current.start();
        }
    }

    public static void main(String args[]) {
        String groupID = "default_field";
        int numberOfConsumers = Integer.parseInt(args[0]);
        String broker = "localhost:8082"; // MUST CHANGE TO ADDRESS OF CV KAFKA SERVER
        String destinationURL = args[1];
        String Authtoken = args[2];
        String topic = args[3];

//        String groupID = "default_field";
//        int numberOfConsumers = 1; // default field
//        String broker = "localhost:8082";
//        String destinationURL = "http://172.24.49.74/webconsole/api/dcube/post/json/4946";
//        String Authtoken = "QSDK 32121b1a9c0a419846d5b69a093cf8efee969e2ec55edb159f523f9b7bea537d4c6df14c41145c2cd938b1abdff4596459ebf63e63e7add1e4d235ec3dcb71d4500780a760647a630db8a36855c25ed9123e01e3fd49547744b1fb5f7599662fd2fc16b5003deeb1460bfc67de8624e390637f29cec96ea374e8597677f551329b000c9378957fc4fefb833a4333c68ed2a4e9f9effc75218bd30073573ad422e61a7f75ec9c75e6353114d4398ede99bc248e2df1153272a85292ed9543f69b98714a7b0c042d458089bc34f0227967bcf3351df05ddf7c1cd91c73e6c4e476a4d8ddc20bf3fdc114f224866c756b3b7e9420205b65178aa238852918e4560c3f7545eb1f7ca1e4d7565a7a5db009c6048eed9c49d93c756";
//        String topic = "jsontest3.json";

        MessengerConsumerGroup consumerGroup = new MessengerConsumerGroup(broker, groupID, topic, numberOfConsumers, destinationURL, Authtoken);
        consumerGroup.runGroup();
    }
}


