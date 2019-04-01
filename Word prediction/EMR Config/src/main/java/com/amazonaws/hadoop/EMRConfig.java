package com.amazonaws.hadoop;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class EMRConfig {

	public static void main(String[] args) {
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
				new ProfileCredentialsProvider().getCredentials());
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct " + "and is in valid format.", e);
		}

		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(credentialsProvider).withRegion("us-east-1").build();

		HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
				.withJar("s3n://akiaj27wiqnm2yse5m2a-dsp-assignment2/wordprediction.jar") // This should be a full map
																							// reduce application.
				//.withArgs("s3n://akiaj27wiqnm2yse5m2a-dsp-assignment2/input/",
				.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data",
						"s3n://akiaj27wiqnm2yse5m2a-dsp-assignment2/out/");

		StepConfig stepConfig = new StepConfig().withName("WordPredictionStep").withHadoopJarStep(hadoopJarStep)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig().withInstanceCount(2)
				.withMasterInstanceType(InstanceType.M1Large.toString())
				.withSlaveInstanceType(InstanceType.M1Large.toString()).withHadoopVersion("2.6.0")
				.withEc2KeyName("sagiv").withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest().withName("WordPredictionJob")
				.withInstances(instances).withSteps(stepConfig)
				.withLogUri("s3n://akiaj27wiqnm2yse5m2a-dsp-assignment2/logs/")
				.withReleaseLabel("emr-5.20.0");
		runFlowRequest.setServiceRole("EMR_DefaultRole");
		runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
		
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}

}
