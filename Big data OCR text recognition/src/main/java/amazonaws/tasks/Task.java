package amazonaws.tasks;
/*
 *	Should be sent throw an AWS sqs.
 * */
public abstract class Task {
	// ******************************* Fields ********************************
	public String appID;
	public String managerID;

	// ******************************* Methods *******************************
	public Task(String appID, String managerID) {
		this.appID = appID;
		this.managerID = managerID;
	}

	// ***********************************************************************
}
