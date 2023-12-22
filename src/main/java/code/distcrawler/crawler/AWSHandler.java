package code.distcrawler.crawler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import code.distcrawler.crawler.info.URLInfo;

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

public class AWSHandler {

	
	private String bucketName = "crawlerdemong12123"; // "crawlerdemog12";
	private String crawlerID = "crawler1"; 
	
	private int id=0;
	private int crawlerMax=1;
	private S3Client s3 = null;
	private DynamoDbClient ddb = null;

    private AmazonSQS sqs=null;
    private String myQueueUrl=null;
    private String[] Queues=null;

	public AWSHandler(String crawlerID, String bucketName) {
		
		this.bucketName = bucketName;
		this.crawlerID = crawlerID;
		Region region = Region.US_EAST_1;
		s3 = S3Client.builder().region(region).build();
		ddb = DynamoDbClient.builder().region(region).build();
		
	}

	public AWSHandler(String crawlerID,String bucketName,int Id, int maxNum) {

		this.crawlerID=crawlerID;
		this.bucketName=bucketName;
		this.id=Id;
		this.crawlerMax=maxNum;
		Region region = Region.US_EAST_1;

		s3 = S3Client.builder().region(region).build();
		
		ddb = DynamoDbClient.builder().region(region).build();
		
		sqs= AmazonSQSClient.builder().withRegion(region.toString()).build();
		/*
		String objectKey = crawlerID + "/" + currentDate + "/" + startNum;
		String objectPath = "/home/cis455/storage/contentFile";
		String result = putS3Object(s3, bucketName, objectKey, objectPath);
		*/
	}
	
	public void initQueue(boolean cont,int id) {
		final Map<String, String> attributes = new HashMap<String, String>();

		// A FIFO queue must have the FifoQueue attribute set to True
		attributes.put("FifoQueue", "true");

		// If the user doesn't provide a MessageDeduplicationId, generate a MessageDeduplicationId based on the content.
		attributes.put("ContentBasedDeduplication", "true");
		List<String> activeQueues;
		if(id==0) {
		 if(!cont) {
			 
			 activeQueues=sqs.listQueues().getQueueUrls();
			 if(activeQueues.size()>0) {
				 for(String queueUrl:activeQueues) {
					 sqs.deleteQueue(queueUrl);
				 }
				 try {
					 System.out.println("Waiting for AWS queue cleanup");
					Thread.sleep(70000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
			 for(int i=0;i<this.crawlerMax;i++) {
				 CreateQueueRequest createQueueRequest = new CreateQueueRequest("crawler"+String.valueOf(i)+".fifo").withAttributes(attributes);
				 sqs.createQueue(createQueueRequest).getQueueUrl();
			 }
			 try {
				 System.out.println("Waiting for AWS queue creation");
				Thread.sleep(80000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		}
		activeQueues=sqs.listQueues().getQueueUrls();
		int counts=0;
		while(activeQueues.size()<this.crawlerMax && counts<6) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			activeQueues=sqs.listQueues().getQueueUrls();
			counts++;
			System.out.println(" Checking queues count="+counts);
		}
		if(activeQueues.size()==this.crawlerMax) {
			this.Queues=new String[this.crawlerMax];
			for(String queueUrl:activeQueues) {
				String[] pathParts=queueUrl.split("/");
				
				System.out.println(pathParts[pathParts.length-1]);
				Pattern pat=Pattern.compile("crawler([0-9]+)\\.fifo");
				Matcher match=pat.matcher(pathParts[pathParts.length-1]);
				if(match.lookingAt()) {
					int index=Integer.valueOf(match.group(1));
					this.Queues[index]=queueUrl;
					if(index==this.id) {
						myQueueUrl=queueUrl;
					}
				}else {
					System.out.println("Was not able to identify queue");
					System.exit(1);
				}
	
			}
		}else {
			System.out.println("Wrong number of active SQS queues in AWS");
			System.exit(1);
		}
		


	}
	public boolean sendURL(CrawlerElement element) {
		ObjectMapper mapper=new ObjectMapper();
		try {
				URLInfo url=new URLInfo(element.normUrl,element.parent);
				MessageDigest md = MessageDigest.getInstance("MD5");
				md.update(url.getHostName().getBytes());
				byte[] hash = md.digest();
				int queueIndex=(new BigInteger(1,hash)).mod(new BigInteger(String.valueOf(this.crawlerMax))).intValue();
				String message=mapper.writerWithDefaultPrettyPrinter().writeValueAsString(element);
				SendMessageRequest sendMessageRequest =
				            new SendMessageRequest(this.Queues[queueIndex],
				                    message);
				
				/*
				 * When you send messages to a FIFO queue, you must provide a
				 * non-empty MessageGroupId.
				 */
				sendMessageRequest.setMessageGroupId("Group1");
				
				// Uncomment the following to provide the MessageDeduplicationId
				//sendMessageRequest.setMessageDeduplicationId("1");
				SendMessageResult sendMessageResult = sqs
				        .sendMessage(sendMessageRequest);
				//String sequenceNumber = sendMessageResult.getSequenceNumber();
				//String messageId = sendMessageResult.getMessageId();
		} catch (JsonProcessingException| NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public CrawlerElement getURL() {
		ObjectMapper mapper=new ObjectMapper();
		CrawlerElement result=null;
		try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            receiveMessageRequest.setMaxNumberOfMessages(1);
            
            // Uncomment the following to provide the ReceiveRequestDeduplicationId
            //receiveMessageRequest.setReceiveRequestAttemptId("1");
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (final Message message : messages) {
            	result=mapper.readValue(message.getBody().getBytes(), CrawlerElement.class);
            }
  

            // Delete the messages.
            //System.out.println("Deleting the messages that we received.\n");
            
            for (final Message message : messages) {
                sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl,
                        message.getReceiptHandle()));
            }
		}catch(IOException e) {
			e.printStackTrace();
			return null;
		}
		return result;
	}
	
	public boolean containsUrl(String hashedUrl) {
		Map<String, AttributeValue> res = getDynamoDBItem(ddb, "newUrl", "HashedUrl", hashedUrl);
		if(res != null && res.size() != 0) {
			System.out.println("Exist!!!!!");
			return true;
		}
		return false;
	}
	
	public boolean addToDDB(String keyVal, String val1, String val2, String val3, String val4) {
		putItemInTable(ddb, "newUrl", "HashedUrl", keyVal, "NormalizedUrl",
				val1, "TotalWords", val2, "Title", val3, "Description", val4);
		return true;
	}

	public String putContentFileIntoS3(String path) {
		String currentTimeStamp = getTimeStamp();
		String objectKey = crawlerID + "/content/" + currentTimeStamp + "contentFile";
		String objectPath = path;
		String res = putS3Object(s3, bucketName, objectKey, objectPath);
		System.out.println(res);
		return null;
	}
	
	public String putPageRankIntoS3(String path) {
		String currentTimeStamp = getTimeStamp();
		String objectKey = crawlerID + "/pagerank/" + currentTimeStamp + "PageRankFile";
		String objectPath = path;
		String res = putS3Object(s3, bucketName, objectKey, objectPath);
		System.out.println(res);
		return null;
	}
	
	public String putMetaFileIntoS3(String path) {
		String currentTimeStamp = getTimeStamp();
		String objectKey = crawlerID + "/metadata/" + currentTimeStamp + "metaFile";
		String objectPath = path;
		String res = putS3Object(s3, bucketName, objectKey, objectPath);
		System.out.println(res);
		return null;
	}
	public static String getDate() {
		String pattern = "MM-dd-yyyy";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
		String date = simpleDateFormat.format(new Date());
		return date;
	}
	
	public static String getTimeStamp() {
		return String.valueOf(System.currentTimeMillis());
	}
	
	public static String MD5Hash(String input) {
		return crypt(input).substring(0, 16);
	}
	
	private static String crypt(String str) {
		if (str == null || str.length() == 0) {
			throw new IllegalArgumentException("String to encript cannot be null or zero length");
		}
		StringBuffer hexString = new StringBuffer();
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			byte[] hash = md.digest();
			for (int i = 0; i < hash.length; i++) {
				if ((0xff & hash[i]) < 0x10) {
					hexString.append("0" + Integer.toHexString((0xFF & hash[i])));
				} else {
					hexString.append(Integer.toHexString(0xFF & hash[i]));
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return hexString.toString();
	}

	// ****************************************************
	// below is for DynamoDB
	// ****************************************************
	public static void putItemInTable(DynamoDbClient ddb, String tableName, 
			String key, String keyVal,
			String attr1, String val1, 
			String attr2, String val2, 
			String attr3, String val3,
			String attr4, String val4) {

		HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();

		// Add all content to the table
		itemValues.put(key, AttributeValue.builder().s(keyVal).build());
		itemValues.put(attr1, AttributeValue.builder().s(val1).build());
		itemValues.put(attr2, AttributeValue.builder().s(val2).build());
		itemValues.put(attr3, AttributeValue.builder().s(val3).build());
		itemValues.put(attr4, AttributeValue.builder().s(val4).build());

		// Create a PutItemRequest object
		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			ddb.putItem(request);
			System.out.println(tableName + " was successfully updated");

		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			
		}
	}
	public  boolean putItemInTable( String tableName, String key, String keyVal,
			String date, String dateVal) {

		HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();

		// Add all content to the table
		itemValues.put(key, AttributeValue.builder().s(keyVal).build());
		itemValues.put(date, AttributeValue.builder().s(dateVal).build());
	
		// Create a PutItemRequest object
		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			this.ddb.putItem(request);
			//System.out.println(tableName + " was successfully updated");

		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			return false;
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			return false;
		}
		return true;
	}
	public  Map<String,AttributeValue> getDynamoDBItem(String tableName, String key, String keyVal){
		return getDynamoDBItem(this.ddb, tableName, key, keyVal);
	}
	public static Map<String, AttributeValue> getDynamoDBItem(DynamoDbClient ddb, String tableName, String key, String keyVal) {

		HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();

		keyToGet.put(key, AttributeValue.builder().s(keyVal).build());

		// Create a GetItemRequest object
		GetItemRequest request = GetItemRequest.builder().key(keyToGet).tableName(tableName).build();

		try {
			Map<String, AttributeValue> returnedItem = ddb.getItem(request).item();
			if (returnedItem != null) {
//				Set<String> keys = returnedItem.keySet();
//				System.out.println("Table Attributes: \n");
//
//				for (String key1 : keys) {
//					System.out.format("%s: %s\n", key1, returnedItem.get(key1).toString());
//				}
				return returnedItem;
			} else {
				System.out.format("No item found with the key %s!\n", key);
				return null;
			}
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			
		}
		
		return null;
	}

	// ****************************************************
	// below is for S3
	// ****************************************************
	public static String putS3Object(S3Client s3, String bucketName, String objectKey, String objectPath) {

		try {
			// Put a file into the bucket
			PutObjectResponse response = s3.putObject(
					PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
					RequestBody.fromBytes(getObjectFile(objectPath)));

			return response.eTag();

		} catch (S3Exception | FileNotFoundException e) {
			System.err.println(e.getMessage());
			
		}
		return "";
	}

	public static byte[] getObjectFile(String path) throws FileNotFoundException {

		byte[] bFile = readBytesFromFile(path);
		return bFile;
	}

	private static byte[] readBytesFromFile(String filePath) {

		FileInputStream fileInputStream = null;
		byte[] bytesArray = null;

		try {
			File file = new File(filePath);
			bytesArray = new byte[(int) file.length()];

			// read file into bytes[]
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytesArray);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bytesArray;
	}
}
