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

        // wait for SYN_ACK from server until 3 attempts
        for(int i = 0; i < MAX_RETRIES; i++)
        {
            try
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.SYN_ACK && response.getSequenceNum() == sequenceNum)
                {
                    System.out.println("[CLIENT] Received SYN_ACK");

                    sequenceNum++;
                    Message ack = new Message(Message.ACK, sequenceNum);
                    sendMessage(ack);
                    System.out.println("[CLIENT] Message[Type = ACK, Sequence Number = " + sequenceNum + "]");

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

    private boolean waitAck(int expectedSeq) throws IOException
    {
        Message response = receiveMessage();

        if(response.getMessageType() == Message.ACK && response.getSequenceNum() == expectedSeq)
        {
            System.out.println("[CLIENT] Received ACK for sequence number = " + expectedSeq);
            return true;
        }

        return false;
    }

    private void retransmitMessage(Message message, int expectedAck) throws IOException
    {
        for(int i = 0; i < MAX_RETRIES; i++)
        {
            sendMessage(message);

            try 
            {
                if(waitAck(expectedAck)) 
                {
                    return;
                }
            }
            catch(SocketTimeoutException e)
            {
                System.out.println("Timeout waiting for ACK, retrying (" + (i + 1) + "/" + MAX_RETRIES + ")");
            }
        }
        throw new IOException("Failed to send message after " + MAX_RETRIES + " retries");
    }

    private void writeChunksToFile(List<byte[]> chunks, String filename) throws IOException
    {
        FileOutputStream fos = null;

        try 
        {
            fos = new FileOutputStream(filename);
            
            for(int i = 0; i < chunks.size(); i++)
            {
                byte[] chunk = chunks.get(i);
                fos.write(chunk);
            }
        }
        finally
        {
            if(fos != null)
            {
                fos.close();
            }
        }
    }

    public boolean downloadFile(String remoteFilename, String localFilename) throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            System.out.println("Cannot start download. Connection is not established");
            return false;
        }

        System.out.println("===== Starting File Download =====");
        System.out.println("Remote filename (source): " + remoteFilename);
        System.out.println("Local filename (destination): " + localFilename);

        // send read request to server
        byte[] filenameInBytes = remoteFilename.getBytes();
        Message readRequest = new Message(Message.DATA, sequenceNum, filenameInBytes);
        System.out.println("Sending read request...");
        sendMessage(readRequest);
        System.out.println("[CLIENT] Message[Type = DATA, Sequence Number = %d" + sequenceNum + " ]");
        sequenceNum++;

        List<byte[]> receivedChunks = new ArrayList<>();
        int expectedSeq = 0;
        boolean complete = false;

        while(!complete)
        {
            try 
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.ERROR)
                {
                    String error = new String(response.getPayload());
                    System.out.println("Received server error message: " + error);
                    return false;
                }

                if(response.getMessageType() == Message.DATA)
                {
                    int recvSeqNum = response.getSequenceNum();
                    int recvPayloadLen = response.getPayloadLen();
                    System.out.println("Received DATA packet: Sequence Number = " + recvSeqNum + ", Payload Length = " + recvPayloadLen);

                    if(recvSeqNum == expectedSeq)
                    {
                        receivedChunks.add(response.getPayload());
                        expectedSeq++;

                        Message ack = new Message(Message.ACK, recvSeqNum);
                        sendMessage(ack);
                        System.out.println("[CLIENT] Message[Type = ACK, Sequence Number = " + recvSeqNum + "]");
                    }
                    else 
                    {
                        System.out.println("Out of order packet. Expexted Sequence Number = " + expectedSeq + ", Received Sequence Number = " + recvSeqNum);
                            
                        // if packet is duplicate
                        if(expectedSeq > 0)
                        {
                            Message ack = new Message(Message.ACK, expectedSeq - 1);
                            sendMessage(ack);
                        }
                    }
                }

                if(response.getMessageType() == Message.FIN)
                {
                    System.out.println("Received FIN message: File download complete");

                    Message fin_ack = new Message(Message.FIN_ACK, response.getSequenceNum());
                    sendMessage(fin_ack);
                    System.out.println("[CLIENT] Message[Type = FIN_ACK, Sequence Number = "+ response.getSequenceNum() + "]");
                    complete = true;
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println("Timeout waiting for data.");

                // request retransmission
                // System.out.println("Request Retransmission");
                break;
            }
        }

        if(complete)
        {
            writeChunksToFile(receivedChunks, localFilename);
            System.out.println("File Downloaded Successfully at filename '" + localFilename + "'");
            System.out.println("Total packet received: " + receivedChunks.size());
            return true;
        }

        return false; 
    }

    public boolean uploadFile(String localFilename, String remoteFilename) throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            System.out.println("Cannot upload file. Connection is not established");
            return false;
        }

        System.out.println("===== Starting File Upload =====");
        System.out.println("Local filename (source): " + localFilename);
        System.out.println("Remote filename (destination): " + remoteFilename);

        File file = new File(localFilename);
        if(!file.exists())
        {
            System.out.println("Error: File not found.");
            return false;
        }

        Message filename = new Message(Message.DATA, sequenceNum, remoteFilename.getBytes());
        sendMessage(filename);
        System.out.println("Filename sent: Sequence Number = " + sequenceNum);

        waitAck(sequenceNum);
        sequenceNum++;

        // to do.


        return false;
    }

    // close the connection using FIN
    public void disconnect() throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            return;
        }

        System.out.println("====== Terminating Connection ======");

        Message fin = new Message(Message.FIN, sequenceNum);
        sendMessage(fin);
        state = ClientState.FIN_WAIT;
        System.out.println("[CLIENT] Message[Type = FIN, Sequence Number = " + sequenceNum + "]");

        for(int i = 0; i < MAX_RETRIES; i++)
        {
            try 
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.FIN_ACK)
                {
                    System.out.println("Received FIN_ACK");
                    state = ClientState.CLOSED;
                    System.out.println("===== Connection Closed ======");
                    return;
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println("Timeout waiting for ACK, retrying (" + (i + 1) + "/" + MAX_RETRIES + ")");
                sendMessage(fin);
            }
        }

        state = ClientState.CLOSED;
        System.out.println("Forced disconnect after timeout.");
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

            boolean flag = false;

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
                        System.out.println("Enter ")
                        break;
                    case 2:

                        break;
                    case 'X':
                    case 'x':
                        flag = true;
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

    private static final int TIMEOUT = 5000; // 5 seconds
    private static final int MAX_RETRIES = 3;
    private static final int BUFFER_SIZE = 1024;
    private static final int MAX_PAYLOAD_SIZE = 1000;
}