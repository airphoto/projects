import com.rabbitmq.client.*;

import java.io.IOException;
import com.google.gson.*;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;
import java.util.*;
import java.io.*;
//import org.bigtesting.interpolatd.*;

/**
 * Created by wyh770406 on 16/6/3.
 */
public class Recv {

    private final static String QUEUE_NAME = "get_webhdfs_download_report";
    private final static Map<String,String> device_map = new HashMap<String,String>() {{
        put("PC", "2");
        put("Mobile", "0");
        put("Tablet", "1");
    }};

    public static void main(String[] args) throws Exception {
        final String shellPath = args[3];
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(args[0]);
        factory.setUsername(args[1]);
        factory.setPassword(args[2]);
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message);
                try {
                    Gson gson = new GsonBuilder()
                            .setPrettyPrinting()
                            .disableHtmlEscaping()
                            .create();
                    JsonParser parser = new JsonParser();
                    JsonObject reqobj = parser.parse(message).getAsJsonObject();

                    String s_beginday = reqobj.get("start_time").toString().replace("\"", "");

                    String s_endday = reqobj.get("end_time").toString().replace("\"", "");
                    String s_platform = reqobj.get("platform").toString().replace("\"", "");

                    String s_type = reqobj.get("type").toString().replace("\"", "");
                    String s_id = reqobj.get("id").toString();

                    Type listType = new TypeToken<List<String>>() {}.getType();

                    JsonArray arr_tagid = reqobj.get("filters").getAsJsonObject().getAsJsonArray("tagid");
                    String s_tagid = getFilterTagid(arr_tagid,listType);

                    JsonArray arr_placementid = reqobj.get("filters").getAsJsonObject().getAsJsonArray("placementid");
                    String s_placementid = getFilterPlacementid(arr_placementid,listType);

                    JsonArray arr_channel = reqobj.get("filters").getAsJsonObject().getAsJsonArray("channel");
                    String s_channel = getFilterChannel(arr_channel,listType);

                    JsonArray arr_device = reqobj.get("filters").getAsJsonObject().getAsJsonArray("device");
                    String s_device = getFilterDevice(arr_device,listType);

                    JsonArray arr_os = reqobj.get("filters").getAsJsonObject().getAsJsonArray("os");
                    String s_os = getFilterOs(arr_os,listType);

                    JsonArray arr_size = reqobj.get("filters").getAsJsonObject().getAsJsonArray("size");
                    String s_size = getFilterOs(arr_size,listType);

                    String s_columns = reqobj.get("report").getAsJsonObject().get("columns").toString().replace("\"", "");

                    //String s_head = reqobj.get("report").getAsJsonObject().get("head").toString().replace("\"", "");

                    String json_data = "{\"platform\":\""+s_platform+"\",\"columns\":\""+s_columns+"\",\"beginday\":\""+s_beginday+"\",\"endday\":\""+s_endday+"\",\"device\":\""+s_device+"\",\"channel\":\""+s_channel+"\",\"size\":\""+s_size+"\",\"os\":\""+s_os+"\",\"tagid\":\""+s_tagid+"\",\"placementid\":\""+s_placementid+"\",\"id\":\""+s_id+"\"}";

                    System.out.println(json_data);
                    executeShell(shellPath, json_data);

                } catch (Exception e) {
                    e.printStackTrace();
                }

                channel.basicAck(envelope.getDeliveryTag(), false);
            }

        };
        channel.basicConsume(QUEUE_NAME, false, consumer);
    }

    public static void executeShell(String shellPath,String json_data){
        // Interpolator<String[]> interpolator = new Interpolator<>();

        String shell = shellPath + " \"" +json_data.replace("\"", "\\\"") +"\"";

          System.out.println(shell);
       // String[] cmd = new String[]{"/bin/sh","-c",shell};
       // Process ps;
       // try {
       //     ps = Runtime.getRuntime().exec(cmd);
       //     System.out.println(ps.waitFor());
       // } catch (Exception e) {
       //     e.printStackTrace();
       // }
        String[] cmd = new String[]{"/bin/sh","-c",shell};
        Process ps;
        try {
            ps = Runtime.getRuntime().exec(cmd);
            InputStream stderr = ps.getErrorStream();
            InputStreamReader isr = new InputStreamReader(stderr);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            System.out.println("<ERROR>");
            while ((line = br.readLine()) != null)
                System.out.println(line);
            System.out.println("</ERROR>");
            int exitVal = ps.waitFor();
            System.out.println("Process exitValue: " + exitVal);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //try {
        //    Process p = new ProcessBuilder("sh", shellPath, json_data).start();
        //    p.waitFor();                // Wait for the process to finish.
        //    System.out.println("Script executed successfully");
        //} catch (Exception e) {
        //    e.printStackTrace();
        //}
    }

    public static String getFilterTagid(JsonArray atagid, Type list_type) {
        String stagid = "";

        if (atagid !=null) {
            List<String> tagidlist = new Gson().fromJson(atagid, list_type);
            stagid = "'" + String.join("','", tagidlist) + "'";
        }

        return stagid;
    }

    public static String getFilterPlacementid(JsonArray aplacementid, Type list_type) {
        String splacementid = "";

        if (aplacementid !=null) {
            List<String> placementidlist = new Gson().fromJson(aplacementid, list_type);
            splacementid = "'" + String.join("','", placementidlist) + "'";
        }

        return splacementid;
    }

    public static String getFilterChannel(JsonArray achannel, Type list_type) {
        String schannel = "";

        if (achannel !=null) {
            List<String> channellist = new Gson().fromJson(achannel, list_type);
            schannel = "'" + String.join("','", channellist) + "'";
        }
        return schannel;
    }

    public static String getFilterDevice(JsonArray adevice, Type list_type) {
        String sdevice = "";

        if (adevice !=null) {
            List<String> devicelist = new Gson().fromJson(adevice, list_type);
            ArrayList<String> new_devicelist = new ArrayList<String>();
            for (String sdevice_val: devicelist){
                new_devicelist.add(device_map.get(sdevice_val));
            }

            sdevice = String.join(",", new_devicelist) ;
        }
        return sdevice;
    }

    public static String getFilterOs(JsonArray aos, Type list_type) {
        String sos = "";

        if (aos !=null) {
            List<String> oslist = new Gson().fromJson(aos, list_type);
            sos = "'" + String.join("','", oslist) + "'";
        }
        return sos;
    }

    public static String getFilterSize(JsonArray asize, Type list_type) {
        String ssize = "";

        if (asize !=null) {
            List<String> sizelist = new Gson().fromJson(asize, list_type);
            ssize = "'" + String.join("','", sizelist) + "'";
        }
        return ssize;
    }
}
