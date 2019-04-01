package amazonaws.utilities;

public class AliveMessage {
	public String appID;
	public String managerID;
	public String message;
	
	public AliveMessage(String appID, String managerID, String message) {
		this.appID = appID;
		this.managerID = managerID;
		this.message = message;
	}
}
