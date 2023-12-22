package code.distcrawler.indexer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.sync.ResponseTransformer;


public class MetaDataParser {
	
	public static void main(String args[]) {
		String metaDataPath = args[0] + "/metaFile";
		String contentFilePath = args[0] + "/s3content";
		ObjectMapper mapper = new ObjectMapper();
		
		System.out.println(metaDataPath);
		
		String bucketName = "cis455g12crawler";
		String crawlerID = "crawler1";
		String currentDate = "2020-04-12";
		String startNum = "1";
		
        Region region = Region.US_EAST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        
        String objectKey = crawlerID + "/" + currentDate + "/" + startNum;
	
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
                ResponseTransformer.toFile(Paths.get("db", "s3content")));
        
		try {
			Path path = Paths.get(metaDataPath);
			byte[] bytes = Files.readAllBytes(path);
			ArrayList<String> metaDataList = new ArrayList<String>();
			StringBuilder sb = null;
			for(int i = 1; i < bytes.length; i++) {
				char ch = (char)bytes[i];
				if(ch == '{') {
					sb = new StringBuilder();
					sb.append(ch);
				}
				else if(ch == '}') {
					sb.append(ch);
					metaDataList.add(sb.toString());
					i++;
				}
				else {
					sb.append(ch);
				}
			}
						
			ArrayList<ContentInfo> infoList = new ArrayList<ContentInfo>();
			for(String jsonStr: metaDataList) {
				ContentInfo contentInfo = mapper.readValue(jsonStr, ContentInfo.class);
				infoList.add(contentInfo);
			}
			
			ArrayList<String> content = new ArrayList<String>();
			for(ContentInfo info: infoList) {
				int startPos = Integer.valueOf(info.start);
				int endPos = Integer.valueOf(info.end);
				
				String contentStr = readContent(contentFilePath, startPos, endPos);
				
				content.add(contentStr);
				System.out.println(contentStr);
			}
						
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	/**
	 * @param path
	 * @param start
	 * @param end
	 * @return 
	 * @throws IOException
	 */
	private static String readContent(String path, int start, int end) throws IOException {
		FileInputStream fis = new FileInputStream(path);
		ByteBuffer bytes = ByteBuffer.wrap(new byte[end - start]);
		fis.getChannel().read(bytes, start);
		fis.close();
		byte[] readBytes = bytes.array();
		System.out.println("***********");
		String content = new String(readBytes);
		return content;
	}
}
