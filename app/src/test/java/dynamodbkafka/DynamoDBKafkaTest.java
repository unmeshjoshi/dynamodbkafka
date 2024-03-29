/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package dynamodbkafka;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.google.common.util.concurrent.Uninterruptibles;
import dynamodbkafka.utils.LocalDynamoDB;
import dynamodbkafka.utils.LocalDynamoDBCotainer;
import dynamodbkafka.utils.LocalKafka;
import dynamodbkafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DynamoDBKafkaTest {
//    private LocalDynamoDB localDynamoDB = new LocalDynamoDB();
    private LocalDynamoDBCotainer localDynamoDB = new LocalDynamoDBCotainer();
    private LocalKafka localKafka = new LocalKafka();
    private String VEHICLE_TOPIC = "vehicleRecords";
    private int numPartitions = 100;

    @Before
    public void setUp() throws Exception {
        setupDynamoDB();
        setupKafka();
    }

    private void setupDynamoDB() throws Exception {
        localDynamoDB.start();
        localDynamoDB.createTables(Arrays.asList(VehicleRecord.class));
    }

    private void setupKafka() {
        localKafka.start();
        localKafka.createTopic(VEHICLE_TOPIC, numPartitions);
    }


    //An example pipeline to update records in dynamodb with optimistic locking and retries.
    @Test
    public void updateWithOptimisticLocking() throws InterruptedException, ExecutionException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(12);
        int noOfMessagesToProduce = 1000;
        String groupId = "VehicleGroup";

        executor.submit(()->
                new Consumer(localKafka, groupId, Arrays.asList(VEHICLE_TOPIC)).consumeVehicleMessages(noOfMessagesToProduce));
        executor.submit(()->
                new Consumer(localKafka, groupId, Arrays.asList(VEHICLE_TOPIC)).consumeVehicleMessages(noOfMessagesToProduce));

        var producedVINFuture = executor.submit(()->
                produceVehicleMessages(noOfMessagesToProduce));

        //start third consumer after some time. It will trigger rebalance
        executor.schedule(()->
                new Consumer(localKafka, groupId, Arrays.asList(VEHICLE_TOPIC)).consumeVehicleMessages(noOfMessagesToProduce),
                2, TimeUnit.SECONDS);

        var producedVINs = producedVINFuture.get();
        TestUtils.waitUntilTrue(
                ()-> verifyRecordsInDynamoDB(producedVINs), "Waiting to consume all the messages", Duration.ofSeconds(5));

    }

    private boolean verifyRecordsInDynamoDB(List<Integer> producedVINs) {
        List<VehicleRecord> dynamoDbRecords = readRecordsFromDynamoDB(producedVINs);
        return producedVINs.equals(dynamoDbRecords.stream().filter(r -> r != null).map(r -> r.getVin()).collect(Collectors.toList()));
    }

    @NotNull
    private List<VehicleRecord> readRecordsFromDynamoDB(List<Integer> producedVINs) {
        DynamoDBMapper mapper = new DynamoDBMapper(localDynamoDB.getClient());
        List<VehicleRecord> dynamoDbRecords = new ArrayList<>();
        for (Integer producedVIN : producedVINs) {
            VehicleRecord record = mapper.load(VehicleRecord.class, producedVIN);
            System.out.println(producedVIN + "=" + record);
            dynamoDbRecords.add(record);
        }
        return dynamoDbRecords;
    }

    class Consumer {
        private static int consumerIdGen;
        private final KafkaConsumer<String, byte[]> consumer;
        private final int consumerId;
        private String groupId;
        private final List<String> topics;
        private LocalKafka kafka;

        public Consumer(LocalKafka localKafka, String groupId, List<String> topics) {
            consumerId = consumerIdGen++;

            this.kafka = localKafka;
            this.groupId = groupId;
            this.topics = topics;
            this.consumer = localKafka.createKafkaConsumer(groupId);
            consumer.subscribe(topics, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("revoked partitions from " + consumerId + "=" + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("assigned partitions to " + consumerId + "=" + partitions);
                }
            });
        }


        private void consumeVehicleMessages(int noOfMessagesProduced) {
            System.out.println("Starting to consume messages from " + consumerId);

            int consumedNoOfMessages = 0;

            while(consumedNoOfMessages < noOfMessagesProduced) {

                ConsumerRecords<String, byte[]> fetchedMessages = consumer.poll(Duration.ofMillis(1000));
                Set<TopicPartition> assignment = consumer.assignment();
                System.out.println("Group Metadata" + consumer.groupMetadata());
                System.out.println("Assignment for consumer = " + assignment);
                consumedNoOfMessages += fetchedMessages.count();
                System.out.println("fetchedMessages = " + consumedNoOfMessages);
                Map<TopicPartition, OffsetAndMetadata> offsets = updateVehicleRecords(fetchedMessages);
                consumer.commitAsync(offsets, new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> committedOffsets, Exception exception) {
                        //TODO: Handle this.
                        System.out.println("Commit callback for offsets = " + committedOffsets + " Exception=" + exception);
                    }
                });
                System.out.println("Committed offsets = " + offsets);
            }
        }

        private Map<TopicPartition, OffsetAndMetadata> updateVehicleRecords(ConsumerRecords<String, byte[]> messages) {
            Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();
            for (ConsumerRecord<String, byte[]> consumerRecord : messages) {
                VehicleMessage vehicleMessage = JsonSerDes.deserialize(consumerRecord.value(), VehicleMessage.class);
                updateVehicleRecordWithRetry(vehicleMessage, consumerRecord.offset());
                processedOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset()));
            }
            return processedOffsets;
        }

        private void updateVehicleRecordWithRetry(VehicleMessage vehicleMessage, long kafkaOffset) {
            while(true) { //keep trying, will eventually
                DynamoDBMapper mapper = new DynamoDBMapper(localDynamoDB.getClient());
                try {
                    updateRecordInDynamoDB(vehicleMessage, kafkaOffset, mapper);
                    return;
                } catch (ConditionalCheckFailedException e) {
                    //wait for random duration from 0 - 200ms before retry.
                    randomDelay();
                    e.printStackTrace();
                    //continue with retry.
                }
            }
        }

        private static void randomDelay() {
            Uninterruptibles.sleepUninterruptibly(new Random().nextLong(0, 200), TimeUnit.MILLISECONDS);
        }

        private static void updateRecordInDynamoDB(VehicleMessage vehicleMessage, long kafkaOffset, DynamoDBMapper mapper) {
            VehicleRecord vehicleRecord = mapper.load(VehicleRecord.class, vehicleMessage.getVin());
            if (vehicleRecord == null) {
                vehicleRecord = new VehicleRecord();
            }

            if (vehicleRecord.offsetAlreadyProcessed(kafkaOffset)) {
                System.out.println("Offset  " + kafkaOffset + " is already processed + skipping it for " + vehicleRecord.getVin());
                return;
            }

            //update vehicle record
            vehicleRecord.setVin(vehicleMessage.getVin());
            vehicleRecord.setTyrePressure(vehicleMessage.getTyrePressure());
            vehicleRecord.setKafkaOffset(kafkaOffset);

            System.out.println("Updating VehicleRecord = " + vehicleRecord.getVin());

            mapper.save(vehicleRecord);
        }

    }



    private List<Integer> produceVehicleMessages(int noOfMessagesToProduce) {
        var producer = localKafka.createKafkaProducer();
        List<Integer> producedVINs = new ArrayList<>();
        for (int i = 1; i <= noOfMessagesToProduce; i++) {
            int VIN = i;
            VehicleMessage message = new VehicleMessage(VIN, new Random().nextInt(100));
            producer.send(new ProducerRecord<>(VEHICLE_TOPIC, String.valueOf(VIN), JsonSerDes.serialize(message)));
            producedVINs.add(VIN);
        }
        producer.flush();
        return producedVINs;
    }
}
