import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * Created by hadoop on 2016/5/26.
 */
public class PostFiles {
    public static String postFile(String filePath) throws ClientProtocolException, IOException {

        File file = new File(filePath);
        InputStream in = PostFiles.class.getClassLoader().getResourceAsStream("source.properties");
        Properties prop = new Properties();
        prop.load(in);
        String url = prop.getProperty("url");

        FileBody bin = null;

        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost(url);
        if(file != null) {
            bin = new FileBody(file);
        }
        String fileName = file.getParent();
        String parent = fileName.substring(fileName.lastIndexOf("/")+1);

        StringBody id = new StringBody(parent.split("_")[0]);
        MultipartEntity reqEntity = new MultipartEntity();

        reqEntity.addPart("id", id);
        reqEntity.addPart("delivery", bin);

        httppost.setEntity(reqEntity);

        System.err.println("执行: " + httppost.getRequestLine());

        HttpResponse response = httpclient.execute(httppost);
        System.err.println("statusCode is " + response.getStatusLine().getStatusCode());
        HttpEntity resEntity = response.getEntity();
        System.err.println("----------------------------------------");
        System.err.println(response.getStatusLine());
        if (resEntity != null) {
            System.err.println("返回长度: " + resEntity.getContentLength());
            System.err.println("返回类型: " + resEntity.getContentType());
        }
        if (resEntity != null) {
            resEntity.consumeContent();
        }
        return null;
    }
}
