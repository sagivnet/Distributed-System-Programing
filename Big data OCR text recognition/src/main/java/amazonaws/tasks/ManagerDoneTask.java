package amazonaws.tasks;

import amazonaws.utilities.S3URL;

/*	From:	Manager	
 * 	To:		LocalApp
 *	Manager Done Task is to deliver LocalApp 
 *	the location of the output file in S3 */
public class ManagerDoneTask extends Task {
	// ******************************* Fields *********************************
	public S3URL result;// Location of the file containing the HTML result in S3
	// ******************************* Methods ********************************

	public ManagerDoneTask(String appID, S3URL result, String managerID) {
		super(appID, managerID);
		this.result = result;
	}
	// ************************************************************************
}
