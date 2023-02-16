package dynamodbkafka.utils;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

import java.util.List;


class DynamoDBProperties {
    String dynamodbEndpoint = "http://localhost:8000";
    String dynamodbRegion = "";
}

public class LocalDynamoDB {
    private DynamoDBProxyServer dynamoDBProxyServer;
    private AmazonDynamoDB amazonDynamoDB;

    public void start() throws Exception {

        AwsDynamoDbLocalTestUtils.initSqLite();
        dynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", "8000"}
        );
        dynamoDBProxyServer.start();
        amazonDynamoDB = createClient();
    }

    private AmazonDynamoDB createClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        new DynamoDBProperties().dynamodbEndpoint, new DynamoDBProperties().dynamodbRegion
                )).build();
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
            dynamoDBProxyServer.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
