package amazonaws.tasks;

import java.net.URL;

/*	From:	Manager	
 * 	To:		Worker
 *	Image Task is to deliver a worker 
 *	the URL of a specific Image (Not in S3) 
 *	 */
public class ImageTask extends Task{
	// ******************************* Fields ********************************
	public URL image;//	Address of an image in the web that need to be parsed.
	// ******************************* Methods *******************************
	public ImageTask(String appID, URL image, String managerID) {
		super(appID, managerID);
		this.image = image;
	}
	// ***********************************************************************
}
