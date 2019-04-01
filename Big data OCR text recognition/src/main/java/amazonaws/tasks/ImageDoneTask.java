package amazonaws.tasks;

import java.net.URL;

/*	From:	Worker
 * 	To:		Manager
 *	Image Done Task is to deliver the manager 
 *	the URL of image and identified text
 *	 */
public class ImageDoneTask extends Task{
	// ******************************* Fields ********************************
	public URL image;// Location of the image the has been parsed.
	public String parsedImaged; //	result of OCR-parser on that image.
	// ******************************* Methods *******************************
	public ImageDoneTask(String appID,URL image,String parsedImaged, String managerID) {
		super(appID, managerID);
		this.image = image;
		this.parsedImaged = parsedImaged;
	}
	// ***********************************************************************
}
