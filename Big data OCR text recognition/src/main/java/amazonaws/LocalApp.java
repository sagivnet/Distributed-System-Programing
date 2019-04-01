package amazonaws;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
//import com.amazonaws.services.mediastoredata.model.PutObjectRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;
import com.google.gson.Gson;

import amazonaws.tasks.ManagerDoneTask;
import amazonaws.tasks.ManagerTask;
import amazonaws.tasks.Task;
import amazonaws.utilities.AliveMessage;
import amazonaws.utilities.S3URL;
import amazonaws.utilities.Util;

public class LocalApp {
	// ******************************* Fields ********************************
	private static AWSCredentialsProvider credentialsProvider;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	private static AmazonEC2 ec2;

	private static String appID;
	private static String managerID;
	private static String bucketName;
	private static String inputFileName;
	private static String outputFileName;
	private static String key;
	private static long n;
	private static boolean verbose = true;
	private static boolean breakNow = false;
	private static final int sleepTime = 100;
	private static AtomicBoolean isProblemWithManager;

	// ******************************* Methods *******************************
	public static void main(String[] args) {
		if (args.length != 3) {
			if (verbose)
				System.out.println(
						"First argument: input file name\nSecond argument: output file name\nThird argument: amount of tasks per worker");
			return;
		}
		init(args);
		if (!checkIfManagerExists())
			startManager();
		S3URL inputURL = uploadUrls();// address of uploaded input file in S3
		ManagerDoneTask mdt = null;
		sendMsgToManager(createManagerTask(inputURL));// add the new ManagerTask to queue
		Thread checkOnManager = new Thread(() -> checkIfManagerStillAlive());
		checkOnManager.start();
		mdt = listenQueue();
		if (mdt == null) {// problem with manager
			sendErrorMsg();
			if (verbose)
				System.out.println("Error message sent");
			checkOnManager.interrupt();
			return;
		}
		checkOnManager.interrupt();
		downloadResult(mdt.result);
	}

	private static void sendErrorMsg() {
		String errorQ = null;
		if (verbose)
			System.out.println("Searching queue...");
		do {
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				if (queueUrl.contains("errorQ")) {
					errorQ = queueUrl;
					break;
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while (errorQ == null);
		if (verbose)
			System.out.println("Sending ERROR message.");
		sqs.sendMessage(new SendMessageRequest(errorQ, encodeMsg(new AliveMessage(appID, managerID, "Error!"))));
	}

	private static void init(String[] args) {
		// Parse input
		inputFileName = args[0];
		outputFileName = args[1];
		n = Long.parseLong(args[2]);

		// credentials load
		credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct " + "and is in valid format.", e);
		}

		appID = UUID.randomUUID().toString();
		s3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		bucketName = (credentialsProvider.getCredentials().getAWSAccessKeyId() + "-dsp-assignment1").toLowerCase();
		isProblemWithManager = new AtomicBoolean(false);
	}

	private static void downloadResult(S3URL outputURL) {
		if (verbose)
			System.out.println("Downloading result");
		S3Object object = s3.getObject(new GetObjectRequest(outputURL.getBucketName(), outputURL.getKey()));
		try {
			readInputStreamAndWrite(object.getObjectContent());
			if (verbose)
				System.out.println("Finished downloading, please check " + outputFileName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void readInputStreamAndWrite(InputStream input) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		File outFile = new File(outputFileName);
		if (!outFile.createNewFile()) {
			outFile.delete();
			outFile.createNewFile();
		}
		FileWriter writer = new FileWriter(outFile);
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			writer.write(line + "\n");
			writer.flush();
		}
		writer.close();
		reader.close();
	}

	private static ManagerTask createManagerTask(S3URL inputURL) {
		return new ManagerTask(appID, n, inputURL, managerID);
	}

	private static String encodeMsg(Object task) {
		Gson gson = new Gson();
		return gson.toJson(task);
	}

	private static ManagerDoneTask decodeManagerDoneTask(String msg) {
		Gson gson = new Gson();
		return gson.fromJson(msg, ManagerDoneTask.class);
	}

	private static ManagerDoneTask listenQueue() {
		String fromManToAppQ = null;
		ManagerDoneTask mdt = null;
		do {
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				if (queueUrl.contains("fromManToAppQ")) {
					fromManToAppQ = queueUrl;
					break;
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while (fromManToAppQ == null);
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromManToAppQ);
		List<Message> messages;
		breakNow = false;
		if (verbose)
			System.out.println("Waiting for response...");
		while (!breakNow) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message msg : messages) {
				if ((mdt = decodeManagerDoneTask(msg.getBody())).appID.equals(appID)) {
					if (verbose)
						System.out.println("Deleting the message from queue");
					String messageRecieptHandle = msg.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(fromManToAppQ, messageRecieptHandle));
					breakNow = true;
					break;
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (isProblemWithManager.get()) {
				if (verbose)
					System.out.println("Error occured, please try again");
				return null;
			}
		}
		return mdt;
	}

	private static void sendMsgToManager(ManagerTask managerTask) {
		String fromAppToManQ = null;
		if (verbose)
			System.out.println("Searching queue...");
		do {
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				if (queueUrl.contains("fromAppToManQ")) {
					fromAppToManQ = queueUrl;
					break;
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while (fromAppToManQ == null);
		if (verbose)
			System.out.println("Sending the file to Queue.");
		sqs.sendMessage(new SendMessageRequest(fromAppToManQ, encodeMsg(managerTask)));
	}

	private static S3URL uploadUrls() {
		createBucketIfNeccessary();
		if (verbose)
			System.out.println("Uploading file to S3");
		File file = new File(inputFileName);
		key = file.getName().replace('\\', '_').replace('/', '_').replace(':', '_');
		int keyN = 2;
		int lastDot = key.lastIndexOf('.');
		boolean shouldVerb = true;
		while (s3.doesObjectExist(bucketName, key)) {
			if (verbose && shouldVerb)
				System.out.println("File name exists, changing name..");
			if (lastDot != -1)
				if (key.charAt(lastDot - 1) == ')')
					key = key.substring(0, key.indexOf('(')) + '(' + keyN++ + key.substring(lastDot - 1);
				else
					key = key.substring(0, lastDot) + "(" + keyN++ + ")" + key.substring(lastDot);
			else if (key.endsWith(")"))
				key = key.substring(0, key.indexOf('(')) + '(' + keyN++ + ')';
			else
				key += "(" + keyN++ + ")";
			lastDot = key.lastIndexOf('.');
			shouldVerb = false;
		}
		PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
		s3.putObject(req);
		if (verbose)
			System.out.println("File upload completed.");
		return new S3URL(bucketName, key);
	}

	private static void createBucketIfNeccessary() {
		if (s3.doesBucketExistV2(bucketName))
			return;
		if (verbose)
			System.out.println("Making a new bucket..");
		s3.createBucket(bucketName);
		if (verbose)
			System.out.println("Done making a bucket.");
	}

	private static void startManager() {
		if (verbose)
			System.out.println("Seems like there's no manager running. Attepmting to start one..");
		managerID = UUID.randomUUID().toString();
		createBucketIfNeccessary();
		RunInstancesRequest request = new RunInstancesRequest();
		request.setInstanceType(InstanceType.T2Micro.toString());
		request.setMinCount(1);
		request.setMaxCount(1);
		request.setImageId("ami-08fe4614a9f89c0ec");
		request.setKeyName("sagiv");
		request.setUserData(getUserDataScript());
		request.withIamInstanceProfile(new IamInstanceProfileSpecification()
				.withArn("arn:aws:iam::123134847552:instance-profile/EC2InstanceRole"));
		request.withSecurityGroupIds("sg-09b3deba964674543"); // so we can connect in ssh
		List<Instance> managerList = ec2.runInstances(request).getReservation().getInstances();
		Instance manager = managerList.get(0);
		CreateTagsRequest tagRequest = new CreateTagsRequest();
		tagRequest = tagRequest.withResources(manager.getInstanceId()).withTags(new Tag("Manager", managerID));
		ec2.createTags(tagRequest);
	}

	private static boolean checkIfManagerExists() {
		boolean done = false;
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		while (!done) {
			DescribeInstancesResult response = ec2.describeInstances(request);
			for (Reservation reservation : response.getReservations()) {
				for (Instance instance : reservation.getInstances()) {
					for (Tag tag : instance.getTags())
						if (tag.getKey().equals("Manager"))
							if (instance.getState().getName().equals("running")
									|| instance.getState().getName().equals("pending")) {
								managerID = tag.getValue();
								return true;
							}
				}
			}

			request.setNextToken(response.getNextToken());

			if (response.getNextToken() == null) {
				done = true;
			}
		}
		return false;
	}

	private static void checkIfManagerStillAlive() {
		String communicationCheckURL = null;
		final int mySleepTime = 1000 * 60;
		boolean isAlive = false;
		final AtomicBoolean timerBool = new AtomicBoolean(true);
		do {
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
				if (queueUrl.contains("communicationCheckQ")) {
					communicationCheckURL = queueUrl;
					break;
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				return;
			}
		} while (communicationCheckURL == null);

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(communicationCheckURL);
		List<Message> messages;
		AliveMessage msg = null;
		while (!Thread.interrupted()) {
			isAlive = false;
			String msgToSend = encodeMsg(new AliveMessage(appID, managerID, "Are you alive?"));
			SendMessageRequest request = new SendMessageRequest(communicationCheckURL, msgToSend);
			sqs.sendMessage(request);

//			try {
//				Thread.sleep(mySleepTime);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			timerBool.set(true);
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {
				  @Override
				  public void run() {
				    timerBool.set(false);
				  }
				}, mySleepTime);
			
			while (timerBool.get()) {
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				for (Message message : messages) {
					msg = Manager.decodeAliveMessage(message.getBody());
					if (msg.appID.equals(appID)) {
						if (msg.message.equals("Yes")) {
							isAlive = true;
							String messageRecieptHandle = message.getReceiptHandle();
							sqs.deleteMessage(new DeleteMessageRequest(communicationCheckURL, messageRecieptHandle));
							timer.cancel();
							timerBool.set(false);
							break;
						}
					}
				}
			}

			if (!isAlive) {
				isProblemWithManager.set(true);
				return;
			}

			if (verbose)
				System.out.println("\tAlive message received from manager: " + managerID);

			try {
				Thread.sleep(mySleepTime);
			} catch (InterruptedException e1) {
				return;
			}
		}

	}

	/***************************************************************************************************/
	// Script for manager EC2
	private static String getUserDataScript() {
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		lines.add("wget https://s3.amazonaws.com/akiaj27wiqnm2yse5m2a-dsp-assignment1/manager.jar");
		lines.add("java -jar manager.jar " + managerID);
		String str = new String(Base64.encode(Util.join(lines, "\n").getBytes()));
		return str;
	}

}