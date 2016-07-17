import com.rabbitmq.client.*;

import java.io.*;

public class Recv {

    private final static String QUEUE_NAME = "get_keyword_estimated_report";

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
        String shell = shellPath+" "+message;
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