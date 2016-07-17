import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by hadoop on 2016/5/26.
 */
public class KeywordRecv {
    private final static String QUEUE_NAME = "get_keywords_delivery_report";

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
                executeShell(shellPath,message);
                channel.basicAck(envelope.getDeliveryTag(), false);
            }

        };
        channel.basicConsume(QUEUE_NAME, false, consumer);
    }

    public static void executeShell(String shellPath,String message){
        //String tmp = "\"{\\\"id\\\":20,\\\"sql\\\":\\\"select keyword,impression,click,round(cost\\\\\\/100,2) as cost,0 as spend,round(click*100\\\\\\/impression,2) as ctr,round(cost*10\\\\\\/impression,2) as cpm from (select keyword,sum(impression) as impression,sum(click)as click,sum(cost) as cost from hq_keywords_report_hour where d>=20160527 and d<=20160527 and adid=87 and platform='youku' group by keyword order by impression desc) t\\\"}\"";
        String shell = shellPath+" "+message.replace(" ",";").replace("\\\\\\/","\\\\/");
        System.out.println(shell);
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
    }
}
