package dynamodbkafka.utils;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

public class LocalDynamoDBCotainer {
    GenericContainer container
            = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local"))
            .withExposedPorts(8000);
    ;
    private AmazonDynamoDB amazonDynamoDB;

    public void start() {
        container.start();
        amazonDynamoDB = createClient();
    }

    private AmazonDynamoDB createClient() {
        String dynamodbEndpoint = getDynamodbEndpoint(container);
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        dynamodbEndpoint, new DynamoDBProperties().dynamodbRegion
                )).build();
    }

    @NotNull
    private String getDynamodbEndpoint(GenericContainer container) {
        int mappedPort = container.getMappedPort(8000);
        String host = "localhost";
        return "http://" + host + ":" + mappedPort;
    }

    public AmazonDynamoDB getClient() {
        return amazonDynamoDB;
    }

    public void createTables(List<Class<?>> mappedClasses) {
        AmazonDynamoDB amazonDynamoDB = getClient();
        var mapper = new DynamoDBMapper(amazonDynamoDB);
        for (Class<?> mappedClass : mappedClasses) {
            var tableRequest = mapper.generateCreateTableRequest(mappedClass);
            tableRequest.setProvisionedThroughput(new ProvisionedThroughput(100L, 100L));
            amazonDynamoDB.createTable(tableRequest);
        }
    }

    public void stop() {
        try {
            container.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
