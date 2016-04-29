package flume;

public class MainPoint {

    public static final String HOST = "localhost";
    public static final int PORT_NUMBER = 44444;

    public static void main(String[] args) throws InterruptedException {

        ErrorRpcClientFacade myFacade = new ErrorRpcClientFacade(HOST, PORT_NUMBER);

        while (true)
        {
            String message = ErrorGenerator.generateString();
            myFacade.sendDataToFlume(message);
            System.out.println("Message was sent: " + message);

            Thread.sleep(1000);
        }
    }
}