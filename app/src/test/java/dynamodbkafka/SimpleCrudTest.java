package dynamodbkafka;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.*;

public class SimpleCrudTest {

  private static AmazonDynamoDBLocal amazonDynamoDBLocal;
  private SimpleCrud simpleCrud;

  @Before
  public void setUpClass() {
    AwsDynamoDbLocalTestUtils.initSqLite();
    amazonDynamoDBLocal = DynamoDBEmbedded.create();

    // Init DB table
    new SimpleCrud(createDocumentInterfaceClient()).initDb(new ProvisionedThroughput(1L, 1L));
  }

  @After
  public void tearDownClass() {
    amazonDynamoDBLocal.shutdown();
  }

  private static DynamoDB createDocumentInterfaceClient() {
    final AmazonDynamoDB client = amazonDynamoDBLocal.amazonDynamoDB();
    return new DynamoDB(client);
  }

  @BeforeEach
  public void setUp() {
    simpleCrud = new SimpleCrud(createDocumentInterfaceClient());
  }


  @Test
  public void when_StoreValueIsCalled_then_CanBeRetrieved() {
    // Given
    final int key = 10;
    final String value = "TEN";
    final String expected = value;

    // When
    simpleCrud.storeValue(10, value);

    // Then
    final String actual = simpleCrud.retrieveValue(key);

    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void when_RetrieveValueCalledWithNotStoredKey_then_ExceptionIsThrown() {
    // Given
    final int key = 15;

    // Then
    Assertions.assertThrows(IllegalStateException.class, () -> simpleCrud.retrieveValue(key), "Excepted exception is not thrown for invalid key");
  }
}
