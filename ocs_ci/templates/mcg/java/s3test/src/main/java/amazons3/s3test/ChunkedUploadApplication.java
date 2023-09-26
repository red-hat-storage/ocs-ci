package amazons3.s3test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import software.amazon.awssdk.utils.Pair;


public class ChunkedUploadApplication {

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException {
		System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

		String endpoint = args[0];
		String ACCESS_KEY = args[1];
		String SECRET_KEY = args[2];
		String bucketName = args[3];
		Boolean isMultipart = false;

		if (args[4] == "True")
			isMultipart = true;
		AmazonS3 s3Client = configureS3Client(endpoint, ACCESS_KEY, SECRET_KEY);
		Boolean testResult = testCycle(s3Client, bucketName, isMultipart); // regular bucket + regular upload
		int failure = 1;
		if (testResult == true) {
			System.out.println("TEST FINISHED SUCCEFULLY!!");
			failure = 0;
		}
		else {
			System.out.println("TEST FAILED!!");
		}
		System.exit(failure);
	}

	public static Boolean testCycle(AmazonS3 s3Client, String bucketName, Boolean multipartUpload)
			throws IOException, NoSuchAlgorithmException {

        // this performs upload operation based on if multi part upload or not
        // then downloads the file to compare the integrity of uploaded and downloaded objects
        // finally cleans up the generated files

		String OriginalFilePath = "TestFile" + Instant.now().toString();
		String localFilePath = "PATH2";
		File localFile = new File(localFilePath); // creates an empty file for the output

		try {
			// creates a file of size 15MB contains random content.
			File originalFile = createRandomContentFile(OriginalFilePath, 15 * 1024 * 1024);
			Boolean uploadSuceeded = multipartUpload
					? multiPartUpload(s3Client, bucketName, OriginalFilePath, originalFile)
					: regularUpload(s3Client, bucketName, OriginalFilePath, originalFile);
 			if (uploadSuceeded == true)
				System.out.println("Uploaded objects successfully");
			else
			{
				System.out.println("Upload objects failed");
				return false;
			}

			Boolean downloadSuceeded = download(s3Client, bucketName, OriginalFilePath, localFile);
			if (downloadSuceeded == true)
				System.out.println("Downloded objects successfully");
			else
			{
				System.out.println("Download objects failed");
				return false;
            }

			Boolean filesAreEqual = compareMD5(OriginalFilePath, localFilePath);
			if (filesAreEqual == true)
				System.out.println("Uploaded & Downloaded files are equal");
			else
			{
				System.out.println("Uploaded & Downloaded files don't match");
				return false;
			}

			Boolean deleteFiles = (new File(OriginalFilePath)).delete() && localFile.delete();
			if (deleteFiles == true)
			    System.out.println("Files are deleted successfully");
			else
			    System.out.println("Files are not deleted successfully");

			return uploadSuceeded && downloadSuceeded && filesAreEqual;
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (SdkClientException e) {
			e.printStackTrace();
		}
		return false;
	}

	// configureS3Client configures s3Client to use noobaa endpoint and access keys
	// and upload files using chunked upload
	public static AmazonS3 configureS3Client(String endpoint, String accessKey, String secretKey) {
		EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endpoint, "us-east-1");
		BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setSignerOverride("AWSS3V4SignerType");

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withEndpointConfiguration(endpointConfiguration).withPathStyleAccessEnabled(true)
				.withPayloadSigningEnabled(true).withClientConfiguration(clientConfiguration).build();

		return s3Client;
	}

	// regularUpload uploads file to bucket using s3Client
	public static Boolean regularUpload(AmazonS3 s3Client, String bucketName, String key, File originalFile) {
		try {
			PutObjectRequest request = new PutObjectRequest(bucketName, key, originalFile);
			s3Client.putObject(request);
			return true;
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (SdkClientException e) {
			e.printStackTrace();
		}
		return false;
	}

	// multiPartUpload uploads file to bucket using s3Client using multipartUpload
	public static Boolean multiPartUpload(AmazonS3 s3Client, String bucketName, String key, File originalFile) {
		long contentLength = originalFile.length();
		long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.
		try {
			List<PartETag> partETags = new ArrayList<PartETag>();

			// init request
			InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key);
			InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

			// Upload the file parts
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				partSize = Math.min(partSize, (contentLength - filePosition)); // last part <= 5MB

				UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName).withKey(key)
						.withUploadId(initResponse.getUploadId()).withPartNumber(i).withFileOffset(filePosition)
						.withFile(originalFile).withPartSize(partSize);

				UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
				partETags.add(uploadResult.getPartETag());
				filePosition += partSize;
			}

			// Complete the multipart upload.
			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, key,
					initResponse.getUploadId(), partETags);
			s3Client.completeMultipartUpload(compRequest);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			return false;
		} catch (SdkClientException e) {

			e.printStackTrace();
			return false;
		}
		return true;
	}

	// download file from bucket using s3Client
	public static Boolean download(AmazonS3 s3Client, String bucketName, String key, File localFile)
			throws IOException {
		try {
			GetObjectRequest request = new GetObjectRequest(bucketName, key);
			s3Client.getObject(request, localFile);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			return false;
		} catch (SdkClientException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// compareMD5 compares MD5s of file downloaded from bucket using s3Client and
	// original file
	public static boolean compareMD5(String path1, String path2) throws IOException, NoSuchAlgorithmException {
		MessageDigest md1 = MessageDigest.getInstance("MD5");
		md1.update(Files.readAllBytes(Paths.get(path1)));
		byte[] digest1 = md1.digest();

		MessageDigest md2 = MessageDigest.getInstance("MD5");
		md2.update(Files.readAllBytes(Paths.get(path2)));
		byte[] digest2 = md2.digest();

		return Arrays.toString(digest1).equals(Arrays.toString(digest2));
	}

	public static File createRandomContentFile(String fileName, long size) throws IOException {
		File file = new File(fileName);
		file.createNewFile();
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		raf.setLength(size);
		raf.close();
		return file;
	}
}
