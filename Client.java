import java.util.*;
import java.net.*;
import java.io.*;

/**
 *  The class Client represents a client in the reliable data transfer protocol over UDP.
 * 
 *  @author Sky Hannah Parado
 *  @author Rhian Claire Yamsuan
 *  @version 1.0
 */
public class Client
{
    // possible states of the client during the protocol lifecycle
    private enum ClientState 
    {
        CLOSED, SYN_SENT, ESTABLISHED, FIN_WAIT;
    }

    // client contructor
    public Client(int clientPort) throws SocketException
    {
        UDPsocket = new DatagramSocket(clientPort);
        UDPsocket.setSoTimeout(TIMEOUT);
        state = ClientState.CLOSED;
        sequenceNum = new Random().nextInt(1000);

        System.out.println("Client port number: " + clientPort);
        System.out.println("Initial sequence number: " + sequenceNum);
    }

    // establish connection with server
    public boolean connect(String serverHost, int serverPort) throws IOException 
    {
        serverAddress = InetAddress.getByName(serverHost);
        this.serverPort = serverPort;

        System.out.println("===== Establishing Connection =====");

        Message syn = new Message(Message.SYN, sequenceNum);
        sendMessage(syn);
        state = ClientState.SYN_SENT;

        System.out.println("[CLIENT] Message[Type = SYN, Sequence Number = " + sequenceNum + "]");

        // wait for SYN_ACK from server until 5 attempts
        for(int i = 0; i < MAX_RETRIES; i++)
        {
            try
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.SYN_ACK && response.getSequenceNum() == sequenceNum)
                {
                    System.out.println("[CLIENT] Received SYN_ACK");

                    sequenceNum++;
                    expectedAckNum = sequenceNum;
                    state = ClientState.ESTABLISHED;

                    System.out.print("===== Connection Established =====");
                    return true;
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println("Timeout waiting for SYN_ACK, retrying (" + (i + 1) + "/" + MAX_RETRIES + ")");
                sendMessage(syn);
            }
        }

        System.out.println("Failed to establish connection after " + MAX_RETRIES + "attempts");
        state = ClientState.CLOSED;

        return false;
    }

    // sends a protocol message to the server over UDP
    private void sendMessage(Message message) throws IOException
    {
        byte[] data = message.convertToBytes();
        DatagramPacket packet = new DatagramPacket(data, data.length, serverAddress, serverPort); 
        UDPsocket.send(packet);
    } 

    // receives a protocol message from the server over UDP
    private Message receiveMessage() throws IOException
    {
        byte[] buffer = new byte[BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        UDPsocket.receive(packet);
        
        return Message.convertToMessage(Arrays.copyOf(packet.getData(), packet.getLength()));
    }

    // closes the client socket and releases system resources
    public void close()
    {
        if(UDPsocket != null && !UDPsocket.isClosed())
        {
            UDPsocket.close();
        }
    }

    public static void main (String[] args)
    {
        System.out.println("===== Starting SENDER program =====");
        Scanner scanner = new Scanner(System.in);
        Client client = null;

        try 
        {
            System.out.print("Enter client port: ");
            int clientPort = scanner.nextInt();
            scanner.nextLine();

            client = new Client(clientPort);

            System.out.print("Enter server host (localhost): ");
            String serverHost = scanner.nextLine();
            
            if(serverHost.isEmpty())
            {
                serverHost = "localhost";
            }

            System.out.println("Enter server port: ");
            int serverPort = scanner.nextInt();
            scanner.nextLine();

            if(!client.connect(serverHost, serverPort))
            {
                System.out.println("Failed to establish connection to server.");
                return;
            }

            boolean flag = true;

            while(!flag)
            {
                System.out.println("\nFile Transfer Functionality Options:");
                System.out.println("[1] Download File");
                System.out.println("[2] Upload File");
                System.out.println("[X] Disconnect");
                System.out.println("Choose option: ");

                String choice = scanner.nextLine();

                /* to be implemented
                switch(choice)
                {
                    case 1:

                        break;
                    case 2:

                        break;
                    case 'X':
                    case 'x':

                        break;
                    default: System.out.println("Invalid option.");
                }
                */
            }
        } 
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
            return;
        }
        finally
        {
            if(client != null)
            {
                client.close();
            }
            scanner.close();
        }
    }

    private DatagramSocket UDPsocket;
    private InetAddress serverAddress;
    private ClientState state;
    private int serverPort;
    private int sequenceNum;
    private int expectedAckNum;

    private static final int TIMEOUT = 2000; // 2 seconds
    private static final int MAX_RETRIES = 5;
    private static final int BUFFER_SIZE = 1024;
    private static final int MAX_PAYLOAD_SIZE = 1000;
}