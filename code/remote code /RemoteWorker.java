package pa3_remote_worker;

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
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
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

public class RemoteWorker {
	static AmazonDynamoDBClient dynamoDB;

	public static void main(String[] args) {
		//AWS Credentials
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

		try {
			String tableName = "metatable";
			String tableName2 = "checkingtable";
			try {
				// Receive messages from mq
				System.out.println("Receiving messages from mq.\n");
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
								
				while (true) {

					List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
					//If the message queue is empty then update in the checking table
					if (messages.size() == 0){
						Map<String, AttributeValue> item = newItemCheck("1");
						PutItemRequest putItemRequest = new PutItemRequest(tableName2, item);
						PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
						break;
					}
					//Read all the messages in the queue
					for (Message msg : messages) {
						try {
							
							String task = msg.getBody();
							String task_id[] = task.split(" ");
							//Put the executed task's ids in the table 
							Map<String, AttributeValue> item = newItem(task_id[0]);
							PutItemRequest putItemRequest = new PutItemRequest(tableName, item)
									.withConditionExpression("attribute_not_exists(taskid)");
							
							PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
						    //Execute the sleep tasks
							Thread.sleep(Long.parseLong(task_id[2]));
							sqs.sendMessage(new SendMessageRequest(myResQueueUrl, task_id[0]));

							// Deleting the message from the queue as it is read
							String mrh = msg.getReceiptHandle();
							sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, mrh));
						} catch (ConditionalCheckFailedException ccfe) {

						}

					}

				}
			} catch (Exception e) {
				System.out.println("Index out of bound exception");
				e.printStackTrace();

			}

		} catch (AmazonServiceException ase) {
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

	// Put an item into the meta table table
	private static Map<String, AttributeValue> newItem(String taskid) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("taskid", new AttributeValue(taskid));

		return item;
	}

	// Put an item into the checkingtable
	private static Map<String, AttributeValue> newItemCheck(String id) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("id", new AttributeValue(id));

		return item;
	}

}
