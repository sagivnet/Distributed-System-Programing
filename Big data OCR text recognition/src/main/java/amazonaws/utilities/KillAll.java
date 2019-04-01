package amazonaws.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import amazonaws.tasks.ManagerDoneTask;

public class KillAll {

	private static AWSCredentialsProvider credentialsProvider;
	private static AmazonSQS sqs;
	private static AmazonEC2 ec2;
	private static String fromAppToManQ;
	private static final int sleepTime = 100;
	
	
	public static void main(String[] args) {
		init();
		findQueue();
		System.out.println("KILLING!!!!!!");
		sqs.sendMessage(new SendMessageRequest(fromAppToManQ, "KillAll"));
		try {
			Thread.sleep(7000); // give manager 7s to kill itself
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		terminateManagers();
		System.out.println("Now killing myself bye");
	}
	
	private static void init() {
		credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "and is in valid format.", e);
		}

		// Queue interface
		sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		//fromAppToManQ = args[0];
	}
	
	private static void terminateManagers() {
		boolean done = false;
		List<Instance> managerInstances = new ArrayList<>();
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		while (!done) {
			DescribeInstancesResult response = ec2.describeInstances(request);
			for (Reservation reservation : response.getReservations()) {
				for (Instance instance : reservation.getInstances()) {
					for (Tag tag : instance.getTags())
						if (tag.getKey().equals("Manager"))
							if (instance.getState().getName().equals("running")) {
								managerInstances.add(instance);
							}
								
				}
			}

			request.setNextToken(response.getNextToken());

			if (response.getNextToken() == null) {
				done = true;
			}
		}
		TerminateInstancesRequest termRequest = new TerminateInstancesRequest();
		List<String> instancesIds = managerInstances.stream().map((Instance::getInstanceId))
				.collect(Collectors.toList());
		termRequest.withInstanceIds(instancesIds);
		ec2.terminateInstances(termRequest);
	}

	private static void findQueue() {
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
	}

}
