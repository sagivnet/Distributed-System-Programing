package amazonaws.tasks;

import amazonaws.utilities.S3URL;

/*	From:	LocalApp	
 * 	To:		Manager
 *	Manager Task is to get a location of a URL list of images in S3 .
 * */
public class ManagerTask extends Task {
	// ******************************* Fields ********************************
	public S3URL images;// Location of the file containing all the image URLs that need to be parsed.
	public Long n;// number of URLs per worker parameter.
	// ******************************* Methods *******************************

	public ManagerTask(String appID, Long n, S3URL images, String managerID) {
		super(appID, managerID);
		this.n = n;
		this.images = images;
	}
	// ***********************************************************************
}
