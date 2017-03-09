package pa3_remote_client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.style.ToStringCreator;

import java.io.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class RemoteClient {
	static AmazonDynamoDBClient dynamoDB;

	public static void main(String[] args) throws InterruptedException {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/home/aswin/.aws/credentials), and is in valid format.", e);
		}

		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);

		dynamoDB = new AmazonDynamoDBClient(credentials);
		dynamoDB.setRegion(usWest2);

		try {

			// Create table if it does not exist yet
			String tableName = "metatable";
			String tableName2 = "checkingtable";

			if (Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is already ACTIVE");
			} else {
				// Create a table with a primary hash key named 'taskid', which
				// holds a string
				CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
						.withKeySchema(new KeySchemaElement().withAttributeName("taskid").withKeyType(KeyType.HASH))
						.withAttributeDefinitions(new AttributeDefinition().withAttributeName("taskid")
								.withAttributeType(ScalarAttributeType.S))
						.withProvisionedThroughput(
								new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
				TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest)
						.getTableDescription();
				System.out.println("Created Table: " + createdTableDescription);

				// Wait for it to become active
				System.out.println("Waiting for " + tableName + " to become ACTIVE...");
				Tables.awaitTableToBecomeActive(dynamoDB, tableName);

				// Describe our new table
				DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
				TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
				System.out.println("Table Description: " + tableDescription);

				}

			// Get the queue url
			String myQueueUrl = sqs.createQueue("mq").getQueueUrl();

			// Get the response queue url
			String myResQueueUrl = sqs.createQueue("resmq").getQueueUrl();

			// List queues
			System.out.println("Listing all queues in your account.\n");
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				System.out.println("  QueueUrl: " + queueUrl);
			}
			System.out.println();

			// Put tasks in the queue by reading the tasks from the file
			System.out.println("Sending a message to mq.\n");
			
			String filename = args[4];
			String line = null;
			int id = 0;
			try {
				FileReader fileReader = new FileReader(filename);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
                //Read the tasks from the file
				while ((line = bufferedReader.readLine()) != null) {
					String task = Integer.toString(id) + " " + line;
					sqs.sendMessage(new SendMessageRequest(myQueueUrl, task));
					id = id + 1;
				}
			} catch (FileNotFoundException ex) {
				System.out.println("Unable to open the file : " + filename);
			} catch (IOException ex) {
				System.out.println("Error reading file :" + filename);
			}
            
		
			System.out.println("Recording the start time");
			Thread.sleep(5000);	
			//Start timer
			long start_time = System.currentTimeMillis();
			
			// Check if in the checkingtable if all the tasks are executed
			HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
			Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString())
					.withAttributeValueList(new AttributeValue().withS("1"));
			scanFilter.put("id", condition);
			ScanRequest scanRequest = new ScanRequest(tableName2).withScanFilter(scanFilter);
			ScanResult scanResult = dynamoDB.scan(scanRequest);
			while (scanResult.getCount() == 0){
			   scanResult = dynamoDB.scan(scanRequest);
			}
			//End timer
			long end_time = System.currentTimeMillis();
			//Elapsed time
			long elapsed_time = end_time - start_time ;
			System.out.println("Time elapsed : " + elapsed_time);
			
		}//try
		
		
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with SQS, such as not "
					+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}
}

 


