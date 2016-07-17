import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
/**
 * Created by hadoop on 2016/5/27.
 */
public class GetKeywordsFilePath {
    public static String getJson(){

        //构造HttpClient的实例
        HttpClient httpClient = new HttpClient();
        //创建GET方法的实例
        GetMethod getMethod = new GetMethod(getProp().getProperty("keywordsPathUrl"));
        //使用系统提供的默认的恢复策略
        getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
        try {
            //执行getMethod
            int statusCode = httpClient.executeMethod(getMethod);
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + getMethod.getStatusLine());
            }
            //读取内容
            byte[] responseBody = getMethod.getResponseBody();
            //处理内容
            return new String(responseBody);
        } catch (HttpException e) {
            //发生致命的异常，可能是协议不对或者返回的内容有问题
            System.out.println("Please check your provided http address!");
            e.printStackTrace();
        } catch (IOException e) {
            //发生网络异常
            e.printStackTrace();
        } finally {
            //释放连接
            getMethod.releaseConnection();
        }
        return "";
    }

/*    public static void main(String[] args){
        System.out.println(getJson("http://dev.dsp.hogic.cn/Admin/Api/Keywords_expand"));
    }*/

    public static Properties getProp(){
        InputStream in = GetKeywordsFilePath.class.getClassLoader().getResourceAsStream("source.properties");
        Properties prop = new Properties();
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return prop;
    }
}
