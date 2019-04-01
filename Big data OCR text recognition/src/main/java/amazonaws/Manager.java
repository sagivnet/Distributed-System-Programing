package amazonaws;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
//import com.amazonaws.AmazonClientException;
//import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.util.Base64;
import com.google.gson.Gson;

import amazonaws.tasks.ImageDoneTask;
import amazonaws.tasks.ImageTask;
import amazonaws.tasks.ManagerDoneTask;
import amazonaws.tasks.ManagerTask;
import amazonaws.tasks.Task;
import amazonaws.utilities.AliveMessage;
import amazonaws.utilities.S3URL;
import amazonaws.utilities.Util;

public class Manager {
	/*********************************************************************************************************************
	 ***************************************************** Fields ********************************************************
	 *********************************************************************************************************************/
//	private static AWSCredentialsProvider credentialsProvider;
	private static String fromAppToManURL, fromManToAppURL, fromManToWorkerURL, fromWorkerToManURL,
			communicationCheckURL, errorURL;
	private static AmazonSQS sqs;
	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static String managerID;
	private static boolean verbose = true;
	private static ConcurrentHashMap<String, ManagerTask> tasks;
	private static final String bucketName = "akiaj27wiqnm2yse5m2a-dsp-assignment1";;
	private static boolean killAll = false;
	private static final int numOfThreads = 10;
	private static final String workerVisibilityTimeout = "900";
	private static final String workerQueueTimeWaitSeoconds = "5";
	private static final String normalVisibilityTimeout = "4";
	private static final String normalQueueTimeWaitSeoconds = "5";
	private static final long sleepTime = 100;

	/*********************************************************************************************************************
	 ***************************************************** Methods *******************************************************
	 *********************************************************************************************************************/
	public static void main(String[] args) {
		if (args.length != 1) {
			if (verbose)
				System.out.println("1 Argument should be receieved.");
			return;
		}
		init(args);
		buildQueues();
		new Thread(() -> iAmAlive()).start();
		new Thread(() -> checkForErrors()).start();
		Thread receiveTasksThread = new Thread(() -> receiveTasks());
		receiveTasksThread.start();
		Thread handleTasksThread = new Thread(() -> handleTasks());
		handleTasksThread.start();
		try {
			handleTasksThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		killQueues();
		if (verbose)
			System.out.println("Manager' Im dead now, bye bye.");

	}

	/**********************************************************************************************************************/
	private static void handleTasks() {
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numOfThreads);
		while (!killAll) {
			for (ManagerTask task : tasks.values()) {
				executor.submit(() -> taskHandler(task));
				tasks.remove(task.appID);
			}
		}
		executor.shutdown();
	}

	/**********************************************************************************************************************/
	private static void taskHandler(ManagerTask task) {

		List<URL> imagesUrls = downloadURLs(task.images);

		List<Instance> workersInstances = startWorkers(imagesUrls.size() / task.n, task.appID);
		imagesUrls.forEach((img) -> sendMsgToWorkersQ(new ImageTask(task.appID, img, managerID)));
		List<ImageDoneTask> results = getResults(imagesUrls.size(), task.appID);
		if (results == null) { // means it got a kill msg while getting results
			killWorkers(task.appID, workersInstances);
			return;
		}

		if (verbose)
			System.out.println("Manger(TH)' Got result for App:" + results.get(0).appID);

		File outputFile = createHTML(results);
		if (verbose)
			System.out.println("Manger(TH)' HTML Output has created.");

		S3URL outputURL = uploadResult(outputFile);
		if (verbose)
			System.out.println("Manger(TH)' HTML Output has uploaded.");

		sendMsgToApp(new ManagerDoneTask(task.appID, outputURL, managerID));
		if (verbose)
			System.out.println("Manger(TH)' A new message to App: " + task.appID + " has uploaded.");

		killWorkers(task.appID, workersInstances);

		if (verbose)
			System.out.println("Manger(TH)'  Im dead now, bye bye.");
	}

	/**********************************************************************************************************************/
	// Generates n Workers
	private static synchronized List<Instance> startWorkers(long n, String appID) {

		long instancesAmount = 0;
		final long MAX_INSTANCES = 20;
		do {
			instancesAmount = 0;
			boolean done = false;
			DescribeInstancesRequest request = new DescribeInstancesRequest();
			while (!done) {
				DescribeInstancesResult response = ec2.describeInstances(request);
				for (Reservation reservation : response.getReservations()) {
					for (Instance instance : reservation.getInstances()) {
						if (!instance.getState().getName().equals("terminated")) {
							instancesAmount++;
						}
					}
				}

				request.setNextToken(response.getNextToken());

				if (response.getNextToken() == null) {
					done = true;
				}
			}
			if (MAX_INSTANCES - instancesAmount < n) {
				if (verbose)
					System.out.println("Cannot start workers. Maximum amount of instances that can be run: "
							+ MAX_INSTANCES + "\nCurrent running amount: " + instancesAmount + "\nAttempting to run: "
							+ n + "\nAvailable amount: " + (MAX_INSTANCES - instancesAmount));
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} while (MAX_INSTANCES - instancesAmount < n);

		if (verbose)
			System.out.println("Manager' Creating " + n + " Workers.");
		RunInstancesRequest request = new RunInstancesRequest();
		List<Instance> instances = null;
		request.setInstanceType(InstanceType.T2Micro.toString());
		request.setMinCount((int) n);
		request.setMaxCount((int) n);
		request.setImageId("ami-08fe4614a9f89c0ec");
		request.setKeyName("sagiv");
		request.setUserData(getWorkerDataScript(appID));
		request.withSecurityGroupIds("sg-09b3deba964674543"); // so we can connect in ssh
		request.withIamInstanceProfile(new IamInstanceProfileSpecification()
				.withArn("arn:aws:iam::123134847552:instance-profile/EC2InstanceRole"));
		instances = ec2.runInstances(request).getReservation().getInstances();
		CreateTagsRequest tagRequest = new CreateTagsRequest();
		for (Instance inst : instances) {
			tagRequest = tagRequest.withResources(inst.getInstanceId()).withTags(new Tag("managerID", managerID),
					new Tag("appID", appID));
			ec2.createTags(tagRequest);
		}
		return instances;
	}

	/**********************************************************************************************************************/
	private static String getWorkerDataScript(String appID) {
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		// lines.add("sudo yum -y install java-1.8.0");
		lines.add("wget https://s3.amazonaws.com/akiaj27wiqnm2yse5m2a-dsp-assignment1/worker.jar");
		lines.add("java -jar worker.jar " + fromManToWorkerURL + " " + fromWorkerToManURL + " " + appID + " "
				+ managerID);
		String str = new String(Base64.encode(Util.join(lines, "\n").getBytes()));
		return str;
	}

	/**********************************************************************************************************************/
	private static void killWorkers(String appID, List<Instance> workersInstances) {
		String msg = "Kill" + appID + " " + managerID;
		SendMessageRequest request = new SendMessageRequest(fromManToWorkerURL, msg);
		if (verbose) {
			System.out.print("Manager' Sending kill message to Workers. ");
			System.out.print("AppID: " + appID + " ");
		}
		sqs.sendMessage(request);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		TerminateInstancesRequest termRequest = new TerminateInstancesRequest();
		List<String> instancesIds = workersInstances.stream().map((Instance::getInstanceId))
				.collect(Collectors.toList());
		termRequest.withInstanceIds(instancesIds);
		ec2.terminateInstances(termRequest);

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromManToWorkerURL);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		for (Message message : messages) {
			if (message.getBody().equals(msg)) {
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
				break;
			}
		}
	}

	/**********************************************************************************************************************/
	private static S3URL uploadResult(File outputFile) {
		if (verbose)
			System.out.println("Uploading file to S3");
		String key = outputFile.getName().replace('\\', '_').replace('/', '_').replace(':', '_');
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
		PutObjectRequest req = new PutObjectRequest(bucketName, key, outputFile);
		s3.putObject(req);
		if (verbose)
			System.out.println("File upload completed.");
		return new S3URL(bucketName, key);
	}

	/**********************************************************************************************************************/
	// Download URL list of images from S3
	private static List<URL> downloadURLs(S3URL images) {
		List<URL> res = new ArrayList<>();
		if (verbose)
			System.out.println("Manger(TH)' Downloading Urls.");

		S3Object object = s3.getObject(new GetObjectRequest(images.getBucketName(), images.getKey()));
		InputStream urlsInStream = object.getObjectContent();
		Scanner s = new Scanner(urlsInStream);
		s.useDelimiter("\n");
		while (s.hasNext()) {
			try {
				String temp = s.next();
				System.out.print(temp);
				res.add(new URL(temp));
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
		s.close();
		try {
			urlsInStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(res);
		return res;
	}

	/**********************************************************************************************************************/
	// Return all done tasks from fromWorkerToMan queue once all tasks have been
	// done
	private static List<ImageDoneTask> getResults(long tasksCount, String appID) {
		long tasksReceivedCount = 1, tasksSkippedCount = 1;
		List<ImageDoneTask> result = new Vector<>();
		List<Message> messages;
		ImageDoneTask doneTask;
		String msgBody = null;
		System.out.println(
				"Manager' Receiving messages from fromWorkerToMan - " + tasksCount + " messages should be received.");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromWorkerToManURL);
		while (tasksCount != result.size()) {
			// checks them and releases them
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				msgBody = message.getBody();
				if (msgBody.contains(managerID)) {
					if (msgBody.contains("Fail File " + appID + " " + managerID)) {
						if (verbose)
							System.out.println("Received a FILE FAIL msg from worker. skipping one. ("
									+ tasksSkippedCount++ + ")\n\t" + msgBody.substring(msgBody.indexOf("\n") + 1));
						tasksCount--;
					} else if (msgBody.contains("Fail OCR " + appID + " " + managerID)) {
						if (verbose)
							System.out.println("Received a OCR FAIL msg from worker. skipping one. ("
									+ tasksSkippedCount++ + ")\n\t" + msgBody.substring(msgBody.indexOf("\n") + 1));
						tasksCount--;
					} else if ((doneTask = decodeImageDoneTask(msgBody)).appID.equals(appID)) {
						if (verbose)
							System.out.println("Manager' Message received. #" + tasksReceivedCount++);
						result.add(doneTask);
					}
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(fromWorkerToManURL, messageRecieptHandle));
				}
			}
			if (killAll)
				return null;
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (verbose)
			System.out.println(
					"Done receiving messages from workers with appid: " + appID + "\nMessages recieved successfully: "
							+ --tasksReceivedCount + "\nMessages failed: " + --tasksSkippedCount);
		return result;
	}

	/**********************************************************************************************************************/
	private static void sendMsgToApp(ManagerDoneTask task) {
		SendMessageRequest request = new SendMessageRequest(fromManToAppURL, encodeMsg(task));
		if (verbose)
			System.out.println("Manager' Sending a message to fromManToAppQ.\n");
		sqs.sendMessage(request);
	}

	/**********************************************************************************************************************/
	private static File createHTML(List<ImageDoneTask> doneTasks) {
		List<String> output = new ArrayList<>();
		String outputString = null;
		output.add("<html><head><title>OCR</title>\n");
		output.add("</head><body>\n");
		for (ImageDoneTask task : doneTasks) {
			output.add("	<p>\n");
			output.add("		<img src=\"");
			output.add(task.image.toString());
			output.add("\"><br>\n");
			output.add("		" + task.parsedImaged + '\n');
			output.add("	</p>\n");
		}
		output.add("</body></html>");
		outputString = Util.join(output, "");

		File outFile = new File("result " + doneTasks.get(0).appID + ".html");
		try {
			if (!outFile.createNewFile()) {
				outFile.delete();
				outFile.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			FileWriter writer = new FileWriter(outFile);
			writer.write(outputString);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return outFile;
	}

	/**********************************************************************************************************************/
	// Creates and uploads a new ImagesTask
	private static void sendMsgToWorkersQ(ImageTask task) {
		String msg = encodeMsg(task);
		SendMessageRequest request = new SendMessageRequest(fromManToWorkerURL, msg);
		if (verbose) {
			System.out.print("Manager' Sending a message to Workers. ");
			System.out.print("AppID: " + task.appID + " ");
			System.out.print("ImgUrl: " + task.image + '\n');
		}
		sqs.sendMessage(request);
	}

	/**********************************************************************************************************************/
	// Listen to fromAppToMan queue. in a different thread so it listens
	// to queue permanently

	private static void receiveTasks() {
		if (verbose)
			System.out.println("Receiving messages from fromAppToMan. (constantly)");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromAppToManURL);
		List<Message> messages;
		ManagerTask task;
		String msgBody = null;
		while (!killAll) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				if ((msgBody = message.getBody()).equals("KillAll")) {
					killAll = true;
					if (verbose)
						System.out.println("Received KillAll msg");
					return;
				}

				task = decodeManagerTask(msgBody);
				if (verbose) {
					System.out.print("Manager' Message recived from LocalApp. ");
					System.out.print("AppID: " + task.appID + " ");
					System.out.print("n: " + task.n + " ");
					System.out.print(
							"ImgsUrl in S3: " + '<' + task.images.getBucketName() + task.images.getKey() + '>' + '\n');
				}
				tasks.put(task.appID, task);
				if (verbose)
					System.out.println("Deleting the message from queue");
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(fromAppToManURL, messageRecieptHandle));
			}
			try {
				Thread.sleep(sleepTime);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**********************************************************************************************************************/
	private static ManagerTask decodeManagerTask(String msg) {
		Gson gson = new Gson();
		return gson.fromJson(msg, ManagerTask.class);
	}

	/**********************************************************************************************************************/
	private static ImageDoneTask decodeImageDoneTask(String msg) {
		Gson gson = new Gson();
		return gson.fromJson(msg, ImageDoneTask.class);
	}

	/**********************************************************************************************************************/
	protected static String encodeMsg(Object task) {
		Gson gson = new Gson();
		return gson.toJson(task);
	}

	/**********************************************************************************************************************/
	private static void buildQueues() {
		CreateQueueRequest createQueueRequest;
		SetQueueAttributesRequest request;
		final int numOfQueues = 6;
		String[] queuesAlive = new String[numOfQueues];
		for (int i = 0; i < numOfQueues; i++)
			queuesAlive[i] = null;

		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
			if (queueUrl.contains("fromAppToMan")) {
				queuesAlive[0] = queueUrl;
			} else if (queueUrl.contains("fromManToApp")) {
				queuesAlive[1] = queueUrl;
			} else if (queueUrl.contains("fromManToWorker")) {
				queuesAlive[2] = queueUrl;
			} else if (queueUrl.contains("fromWorkerToMan")) {
				queuesAlive[3] = queueUrl;
			} else if (queueUrl.contains("communicationCheck")) {
				queuesAlive[4] = queueUrl;
			} else if (queueUrl.contains("error")) {
				queuesAlive[5] = queueUrl;
			}
		}

		if ((fromAppToManURL = queuesAlive[0]) == null) {
			// Create fromAppToManQ
			if (verbose)
				System.out.println("Manager' Creating a new SQS queue called fromAppToManQ.");
			createQueueRequest = new CreateQueueRequest("fromAppToManQ" + UUID.randomUUID());
			fromAppToManURL = sqs.createQueue(createQueueRequest).getQueueUrl();

			request = new SetQueueAttributesRequest().withQueueUrl(fromAppToManURL)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), normalVisibilityTimeout)
					.addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
							normalQueueTimeWaitSeoconds);
			sqs.setQueueAttributes(request);
		} else if (verbose)
			System.out.println("Manager' connecting to an existing SQS queue called fromAppToManQ.");

		if ((fromManToAppURL = queuesAlive[1]) == null) {
			// Create fromManToAppQ
			if (verbose)
				System.out.println("Manager' Creating a new SQS queue called fromManToAppQ.");
			createQueueRequest = new CreateQueueRequest("fromManToAppQ" + UUID.randomUUID());
			fromManToAppURL = sqs.createQueue(createQueueRequest).getQueueUrl();

			request = new SetQueueAttributesRequest().withQueueUrl(fromManToAppURL)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), normalVisibilityTimeout)
					.addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
							normalQueueTimeWaitSeoconds);
			sqs.setQueueAttributes(request);
		} else if (verbose)
			System.out.println("Manager' connecting to an existing SQS queue called fromManToAppQ.");

		if ((fromManToWorkerURL = queuesAlive[2]) == null) {
			// Create fromManToWorkerQ
			if (verbose)
				System.out.println("Manager' Creating a new SQS queue called fromManToWorkerQ.");
			createQueueRequest = new CreateQueueRequest("fromManToWorkerQ" + UUID.randomUUID());
			fromManToWorkerURL = sqs.createQueue(createQueueRequest).getQueueUrl();

			request = new SetQueueAttributesRequest().withQueueUrl(fromManToWorkerURL)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), workerVisibilityTimeout)
					.addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
							workerQueueTimeWaitSeoconds);
			sqs.setQueueAttributes(request);
		} else if (verbose)
			System.out.println("Manager' connecting to an existing SQS queue called fromManToWorkerQ.");

		if ((fromWorkerToManURL = queuesAlive[3]) == null) {
			// Create fromWorkerToManQ
			if (verbose)
				System.out.println("Manager' Creating a new SQS queue called fromWorkerToManQ.");
			createQueueRequest = new CreateQueueRequest("fromWorkerToManQ" + UUID.randomUUID());
			fromWorkerToManURL = sqs.createQueue(createQueueRequest).getQueueUrl();
			request = new SetQueueAttributesRequest().withQueueUrl(fromWorkerToManURL)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), normalVisibilityTimeout)
					.addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
							normalQueueTimeWaitSeoconds);
			sqs.setQueueAttributes(request);
		} else if (verbose)
			System.out.println("Manager' connecting to an existing SQS queue called fromWorkerToManQ.");

		if ((communicationCheckURL = queuesAlive[4]) == null) {
			if (verbose)
				System.out.println("Manager' Creating a new SQS queue called communicationCheckQ.");
			createQueueRequest = new CreateQueueRequest("communicationCheckQ" + UUID.randomUUID());
			communicationCheckURL = sqs.createQueue(createQueueRequest).getQueueUrl();
			request = new SetQueueAttributesRequest().withQueueUrl(communicationCheckURL)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), normalVisibilityTimeout)
					.addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
							normalQueueTimeWaitSeoconds);
			sqs.setQueueAttributes(request);
		} else if (verbose)
			System.out.println("Manager' connecting to an existing SQS queue called communicationCheckQ.");

		if ((errorURL = queuesAlive[5]) == null) {
			if (verbose)
				System.out.println("Manager' Creating a new SQS queue called errorQ.");
			createQueueRequest = new CreateQueueRequest("errorQ" + UUID.randomUUID());
			errorURL = sqs.createQueue(createQueueRequest).getQueueUrl();
			request = new SetQueueAttributesRequest().withQueueUrl(errorURL)
					.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), normalVisibilityTimeout)
					.addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
							normalQueueTimeWaitSeoconds);
			sqs.setQueueAttributes(request);
		} else if (verbose)
			System.out.println("Manager' connecting to an existing SQS queue called errorQ.");

	}

	/**********************************************************************************************************************/
	private static void killQueues() {

		if (verbose)
			System.out.println("Manager' Deleting fromAppToMan queue.");
		sqs.deleteQueue(new DeleteQueueRequest(fromAppToManURL));
		if (verbose)
			System.out.println("Manager' Deleting fromManToApp queue.");
		sqs.deleteQueue(new DeleteQueueRequest(fromManToAppURL));
		if (verbose)
			System.out.println("Manager' Deleting fromManToWorker queue.");
		sqs.deleteQueue(new DeleteQueueRequest(fromManToWorkerURL));
		if (verbose)
			System.out.println("Manager' Deleting fromWorkerToMan queue.");
		sqs.deleteQueue(new DeleteQueueRequest(fromWorkerToManURL));
		if (verbose)
			System.out.println("Manager' Deleting communicationCheck queue.");
		sqs.deleteQueue(new DeleteQueueRequest(communicationCheckURL));
		if (verbose)
			System.out.println("Manager' Deleting error queue.");
		sqs.deleteQueue(new DeleteQueueRequest(errorURL));
	}

	/**********************************************************************************************************************/
	private static void init(String[] args) {
		// credentials load

//		credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
//		try {
//			credentialsProvider.getCredentials();
//		} catch (Exception e) {
//			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
//					+ "Please make sure that your credentials file is at the correct "
//					+ "location (/home/sagivnet/.aws/credentials), and is in valid format.", e);
//		}

		// Queue interface
//		sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
////		 EC2 interface
//		ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
////		 S3 interface
//		s3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		tasks = new ConcurrentHashMap<>();

		sqs = AmazonSQSClientBuilder.defaultClient();
		s3 = AmazonS3ClientBuilder.defaultClient();
		ec2 = AmazonEC2ClientBuilder.defaultClient();

		managerID = args[0];
//		bucketName = (credentialsProvider.getCredentials().getAWSAccessKeyId() + "-dsp-assignment1").toLowerCase();
	}

	private static void handleError(String errorManagerID) {
		if (verbose)
			System.out.println("Handling error for failed manager: " + errorManagerID);
		sqs.sendMessage(new SendMessageRequest(fromManToWorkerURL, ("Kill" + errorManagerID)));
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// terminate failed manager instance
		boolean done = false;
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		while (!done) {
			DescribeInstancesResult response = ec2.describeInstances(request);
			for (Reservation reservation : response.getReservations()) {
				for (Instance instance : reservation.getInstances()) {
					for (Tag tag : instance.getTags())
						if (tag.getKey().equals("Manager") && tag.getValue().equals(errorManagerID)) {
							if (instance.getState().getName().equals("running")) {
								TerminateInstancesRequest termRequest = new TerminateInstancesRequest();
								termRequest.withInstanceIds(instance.getInstanceId());
								ec2.terminateInstances(termRequest);
							} // terminate failed manager's workers
						} else if (tag.getKey().equals("managerID") && tag.getValue().equals(errorManagerID)) {
							TerminateInstancesRequest termRequest = new TerminateInstancesRequest();
							termRequest.withInstanceIds(instance.getInstanceId());
							ec2.terminateInstances(termRequest);
							break;
						}

				}
			}

			request.setNextToken(response.getNextToken());

			if (response.getNextToken() == null) {
				done = true;
			}
		}

		// delete unnecessary messages from fromManToWorkerQ and from
		// fromWorkerToManQ, and from fromAppToManQ, communicationQ

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromManToWorkerURL);
		receiveMessageRequest.setVisibilityTimeout(10);
		List<Message> messages = null;
		AtomicBoolean timerBool = new AtomicBoolean(true);
		long timerTime = 1000 * 60;
		Timer t = makeTimer(timerBool, timerTime);
		while (timerBool.get()) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				if (message.getBody().contains(errorManagerID)) {
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
					t = resetTimer(timerBool, timerTime, t);
				}
			}
		}

		messages.clear();
		timerBool.set(true);
		t = resetTimer(timerBool, timerTime, t);
		receiveMessageRequest = new ReceiveMessageRequest(fromWorkerToManURL);
		receiveMessageRequest.setVisibilityTimeout(10);
		while (timerBool.get()) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				if (message.getBody().contains(errorManagerID)) {
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(fromWorkerToManURL, messageRecieptHandle));
					t = resetTimer(timerBool, timerTime, t);
				}
			}
		}

		messages.clear();
		timerBool.set(true);
		t = resetTimer(timerBool, timerTime, t);
		receiveMessageRequest = new ReceiveMessageRequest(fromAppToManURL);
		receiveMessageRequest.setVisibilityTimeout(10);
		while (timerBool.get()) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				if (message.getBody().contains(errorManagerID)) {
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(fromAppToManURL, messageRecieptHandle));
					t = resetTimer(timerBool, timerTime, t);
				}
			}
		}

		messages.clear();
		timerBool.set(true);
		t = resetTimer(timerBool, timerTime, t);
		receiveMessageRequest = new ReceiveMessageRequest(communicationCheckURL);
		receiveMessageRequest.setVisibilityTimeout(10);
		while (timerBool.get()) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				if (message.getBody().contains(errorManagerID)) {
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(communicationCheckURL, messageRecieptHandle));
					t = resetTimer(timerBool, timerTime, t);
				}
			}
		}

	}

	private static Timer makeTimer(AtomicBoolean timerBool, long timerTime) {
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				timerBool.set(false);
			}
		}, timerTime);
		return timer;
	}

	private static Timer resetTimer(AtomicBoolean timerBool, long timerTime, Timer oldTimer) {
		oldTimer.cancel();
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				timerBool.set(false);
			}
		}, timerTime);
		return timer;
	}

	private static void checkForErrors() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(errorURL);
		List<Message> messages;
		AliveMessage msg = null;
		final int sleepTime = 1000 * 60 * 2;
		while (true) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				msg = decodeAliveMessage(message.getBody());
				if (msg.message.equals("Error!")) {
					handleError(msg.managerID);
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(errorURL, messageRecieptHandle));
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static void iAmAlive() {
		final int sleepTime = 2000;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(communicationCheckURL);
		List<Message> messages;
		AliveMessage msg = null;
		while (true) {
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				msg = decodeAliveMessage(message.getBody());
				if (msg.managerID.equals(managerID)) {
					if (msg.message.equals("Are you alive?")) {
						String msgToSend = encodeMsg(new AliveMessage(msg.appID, managerID, "Yes"));
						SendMessageRequest request = new SendMessageRequest(communicationCheckURL, msgToSend);

						sqs.sendMessage(request);
						if (verbose)
							System.out.println("\tCalming " + msg.appID);
						String messageRecieptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(communicationCheckURL, messageRecieptHandle));
					}
				}
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	protected static AliveMessage decodeAliveMessage(String msg) {
		Gson gson = new Gson();
		return gson.fromJson(msg, AliveMessage.class);
	}

	/**********************************************************************************************************************/
	/**********************************************************************************************************************/
}