package amazonaws;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.io.FileUtils;

//import com.amazonaws.auth.AWSCredentialsProvider;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;

import amazonaws.tasks.ImageDoneTask;
import amazonaws.tasks.ImageTask;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import net.sourceforge.tess4j.util.LoadLibs;

/*
 * Worker job is to work on 1 image at a time. 
 */
public class Worker {
	/*********************************************************************************************************************
	 ***************************************************** Fields ********************************************************
	 *********************************************************************************************************************/
	private static AmazonSQS sqs;
//	private static AWSCredentialsProvider credentialsProvider;
	private static String fromManToWorkerURL, fromWorkerToManURL, appID, managerID;
	private static boolean verbose = true;
	private static final int sleepTime = 1000;
	private static ITesseract instance;
	private static File tessDataFolder;
	private static String messageRecieptHandle;
	private static AtomicBoolean foundKill;
	private static final String killMsg1 = "Kill" + appID + " " + managerID;
	private static final String killMsg2 = "Kill" + managerID;
	private static String queueLock = new String("This is a queue lock.");

	/*********************************************************************************************************************
	 ***************************************************** Methods *******************************************************
	 *********************************************************************************************************************/
	/*
	 * args[0] = fromManToWorker queue URL args[1] = fromWorkerToMan queue URL
	 * args[2] = specific appID args[3] = managerID
	 */
	public static void main(String[] args) {
		init(args);
		ImageTask task;
		String parsedImg;
		File imgFile;
		Thread mainThread = Thread.currentThread();
		new Thread(() -> lookForKillMessage(mainThread)).start();
		while (!Thread.interrupted()) {
			if ((task = receiveTask()) == null) // interupted.. means should be killed
				return;
			if ((imgFile = downloadImgFromUrl(task.image)) == null) { // download failed
				sendFailMsg("File", task.image.toString());
				if (verbose)
					System.out.println("Deleting the message from queue");
				sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
				continue;
			}
			if ((parsedImg = applyOcr(imgFile)) == null) { // ocr failed for some reason, continue(?)
				imgFile.delete();
				sendFailMsg("OCR", task.image.toString());
				if (verbose)
					System.out.println("Deleting the message from queue");
				sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
				continue;
			}
			imgFile.delete();
			sendMsgToManager(new ImageDoneTask(task.appID, task.image, parsedImg, managerID));
			if (verbose)
				System.out.println("Deleting the message from queue");
			sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
		}
		if (verbose)
			System.out.println("Worker' Im dead now, bye bye.");
	}

	private static void lookForKillMessage(Thread mainThread) {
		if (verbose)
			System.out.println("Worker' Receiving messages from fromManToWorker. (looking for kill)");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromManToWorkerURL);
		receiveMessageRequest.setVisibilityTimeout(1);
		List<Message> messages;
		foundKill.set(false);
		while (!foundKill.get()) {
			synchronized (queueLock) {
				if (foundKill.get()) //happens if receivetasks gets the kill message first
					return;
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				for (Message message : messages) {
					if (message.getBody().equals(killMsg1) || message.getBody().equals(killMsg2)) {
						foundKill.set(true);
						sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
						// kill main thread
						mainThread.interrupt();
						return;
					}
				}
			}
			if (!foundKill.get())
				try {
					Thread.sleep(sleepTime);
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}

	/**********************************************************************************************************************/
	private static void init(String args[]) {

		fromManToWorkerURL = args[0];
		fromWorkerToManURL = args[1];
		appID = args[2];
		managerID = args[3];
		// Credentials load
//		credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
//
//		// Queue interface
//		sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		sqs = AmazonSQSClientBuilder.defaultClient();
		instance = new Tesseract();
		tessDataFolder = LoadLibs.extractTessResources("tessdata"); // Maven build bundles English data
		instance.setDatapath(tessDataFolder.getPath());

	}

	/**********************************************************************************************************************/
	private static void sendMsgToManager(ImageDoneTask task) {
		String msg = Manager.encodeMsg(task);
		SendMessageRequest request = new SendMessageRequest(fromWorkerToManURL, msg);
		if (verbose) {
			System.out.println("Worker' Sending a message to Manager. ");
			System.out.println("\t AppID: " + task.appID + " ");
			System.out.println("\t ImgUrl: " + task.image + " ");
			// System.out.println("\t Parsed: " + task.parsedImaged + '\n' + "#End Of Parsed
			// Image#");
		}
		sqs.sendMessage(request);

	}

	/**********************************************************************************************************************/
	private static ImageTask receiveTask() {
		messageRecieptHandle = null;
		if (verbose)
			System.out.println("Worker' Receiving messages from fromManToWorker.");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fromManToWorkerURL);
		List<Message> messages;
		ImageTask task = null;
		boolean foundMsg = false;
		String msgBody = null;
		while (!foundMsg) {
			synchronized (queueLock) {
				if (Thread.interrupted()) {
					Thread.currentThread().interrupt();
					return null;
				}
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				for (Message message : messages) {
					msgBody = message.getBody();
					if (msgBody.equals(killMsg1) || msgBody.equals(killMsg2)) {
						messageRecieptHandle = message.getReceiptHandle();
						foundKill.set(true); // also kills lookForKillMessageThread
						sqs.deleteMessage(new DeleteMessageRequest(fromManToWorkerURL, messageRecieptHandle));
						sqs.sendMessage(new SendMessageRequest(fromWorkerToManURL, msgBody));
						// kill main thread
						return null;
					}
					if ((task = decodeMsg(msgBody)).appID.equals(appID)) {
						if (verbose) {
							System.out.print("Worker' Message received from Manager. ");
							System.out.println("\t AppID: " + task.appID + " ");
							System.out.println("\t Image URL: " + task.image);
						}
						messageRecieptHandle = message.getReceiptHandle();
						foundMsg = true;
						break;
					}
				}
			}
			if (!foundMsg)
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException ie) {
					return null;
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		return task;
	}

	/**********************************************************************************************************************/
	private static ImageTask decodeMsg(String msg) {
		Gson gson = new Gson();
		return gson.fromJson(msg, ImageTask.class);
	}

	/**********************************************************************************************************************/
	private static File downloadImgFromUrl(URL url) {
		File imgFile = new File("image");
		File newImgFile = null;
		final int connectionTimeout = 15000, readTimeout = 15000; // maybe adjust better
		if (verbose)
			System.out.println("Downloading file");
		try {
			FileUtils.copyURLToFile(url, imgFile, connectionTimeout, readTimeout);
			if (verbose)
				System.out.println("Download complete");
		} catch (IOException e) {
			e.printStackTrace();
			imgFile.delete();
			return null;
		}
		try {
			newImgFile = new File(imgFile.getPath() + "." + fileEnding(imgFile));
			FileInputStream fstream = new FileInputStream(imgFile);
			FileChannel src = fstream.getChannel();
			FileOutputStream foutStream = new FileOutputStream(newImgFile);
			FileChannel dest = foutStream.getChannel();
			dest.transferFrom(src, 0, src.size());
			src.close();
			dest.close();
			fstream.close();
			foutStream.close();
			imgFile.delete();
		} catch (IOException e) {
			e.printStackTrace();
			imgFile.delete();
			newImgFile.delete();
			newImgFile = null;
		}
		return newImgFile;
	}

	/**********************************************************************************************************************/

	public static String applyOcr(File imageFile) {
		String result = null;
		try {
			result = instance.doOCR(imageFile);
//			if (verbose)
//				System.out.println(result);
		} catch (TesseractException e) {
			System.err.println(e.getMessage());
			result = null;
		}
		return result;
	}

	/**********************************************************************************************************************/

	public static void sendFailMsg(String typeOfFail, String link) {
		String msg = "Fail " + typeOfFail + " " + appID + " " + managerID + "\n" + link;
		SendMessageRequest request = new SendMessageRequest(fromWorkerToManURL, msg);
		if (verbose) {
			System.out.println("Worker' Sending a FAIL message to Manager. ");
			System.out.println("\t AppID: " + appID + " ");
		}
		sqs.sendMessage(request);
	}

	public static String fileEnding(File f) throws IOException {
		ImageInputStream iis = ImageIO.createImageInputStream(f);
		String formatName = null;

		Iterator<ImageReader> imageReaders = ImageIO.getImageReaders(iis);

		while (imageReaders.hasNext()) {
			ImageReader reader = (ImageReader) imageReaders.next();
			formatName = reader.getFormatName();
		}
		return formatName;
	}
	/**********************************************************************************************************************/
	/**********************************************************************************************************************/
}
