package amazonaws.utilities;
public class S3URL 
{
	private String bucketName;
	private String key;
	
	public S3URL (String bucketName,String key)
	{
		this.bucketName = bucketName;
		this.key = key;
	}
	
	public String getBucketName()
	{
		return bucketName;
	}
	
	public String getKey()
	{
		return key;
	}
}
