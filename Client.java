import java.util.*;
import java.net.*;
import java.io.*;

//last update: Feb 21, 2026 10:05am

/**
 *  The class Client represents a client in the reliable data transfer protocol over UDP.
 *  It supports session establishment (three-way handshake), file download, file upload, and clean session termination.
 *  
 *  Each DATA packet is acknowledged individually before the next one is sent. 
 *  Lost packets are detected via timeout and retransmitted up to MAX_RETRIES times.
 * 
 *  @author Sky Hannah Parado
 *  @author Rhian Claire Yamsuan
 *  @version 1.0
 */
public class Client
{
    /**
     * Client states as defined by the protocol state machine.
     *
     *  CLOSED      — no active session
     *  SYN_SENT    — SYN sent, waiting for SYN_ACK
     *  ESTABLISHED — session active, ready for file operations
     *  FIN_WAIT    — FIN sent, waiting for FIN_ACK
     */
    private enum ClientState 
    {
        CLOSED, SYN_SENT, ESTABLISHED, FIN_WAIT;
    }

    /**
     * Creates a Client bound to the given local port.
     * The initial SeqNum (ISN) is fixed at 1 for reproducibility.
     * @param clientPort  local UDP port to bind to
     * @throws SocketException if the socket cannot be created
     */
    public Client(int clientPort) throws SocketException
    {
        UDPsocket = new DatagramSocket(clientPort);
        UDPsocket.setSoTimeout(TIMEOUT);
        state = ClientState.CLOSED;
        sequenceNum = 0;

        System.out.println("[CLIENT] Port Number = " + clientPort + " | Initial SeqNum: " + sequenceNum);
    }

    /**
     * Performs the three-way handshake to establish a session with the server.
     *
     * Handshake flow:
     *   1. Client sends SYN(ISN)
     *   2. Server responds with SYN_ACK(ISN)
     *   3. Client sends ACK(ISN+1)
     *
     * After a successful handshake, session parameters are printed:
     * both sides have agreed on the ISN, max payload size, timeout, and retries.
     *
     * @param serverHost  hostname or IP of the server
     * @param serverPort  UDP port the server is listening on
     * @return true if connection was established, false otherwise
     */

    public boolean connect(String serverHost, int serverPort) throws IOException 
    {
        serverAddress = InetAddress.getByName(serverHost);
        this.serverPort = serverPort;

        System.out.println(Colors.cyan("\n===== Establishing Connection ====="));

        Message syn = new Message(Message.SYN, sequenceNum);
        sendMessage(syn);
        state = ClientState.SYN_SENT;

        System.out.println("[CLIENT] Message sent [Type = SYN, SeqNum = " + sequenceNum + "]");

        // wait for SYN_ACK from server until 3 attempts
        for(int i = 0; i < MAX_RETRIES; i++)
        {
            try
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.SYN_ACK && response.getSequenceNum() == sequenceNum)
                {
                    System.out.println("[CLIENT] Received SYN_ACK, SeqNum = " + response.getSequenceNum());

                    sequenceNum++;
                    Message ack = new Message(Message.ACK, sequenceNum);
                    sendMessage(ack);
                    System.out.println("[CLIENT] Message sent [Type = ACK, SeqNum = " + sequenceNum + "]");

                    expectedAckNum = sequenceNum;
                    state = ClientState.ESTABLISHED;

                    System.out.println(Colors.bold(Colors.green("CONNECTION ESTABLISHED")));
                    System.out.println("\n[CLIENT] Session parameters:");
                    System.out.println("         Server      : " + serverHost + ":" + serverPort);
                    System.out.println("         ISN         : " + (sequenceNum - 1));
                    System.out.println("         Max payload : " + MAX_PAYLOAD_SIZE + " bytes");
                    System.out.println("         Timeout     : " + TIMEOUT + " ms");
                    System.out.println("         Max retries : " + MAX_RETRIES);

                    return true;
                }
                else
                {
                    System.out.println(Colors.yellow("[CLIENT] Unexpected response during handshake: " + response.msgTypeString()));
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println(Colors.yellow("Timeout waiting for SYN_ACK, retrying (" + (i + 1) + "/" + MAX_RETRIES + ")"));
                sendMessage(syn); // retransmit SYN
            }
        }

        System.out.println(Colors.red("Failed to establish connection after " + MAX_RETRIES + "attempts"));
        state = ClientState.CLOSED;

        return false;
    }

    /**
     * WORKING: Serializes and sends a Message to the server.
    
    private void sendMessage(Message message) throws IOException
    {
        byte[] data = message.convertToBytes();
        data = Cryptography.encrypt(data);
        DatagramPacket packet = new DatagramPacket(data, data.length, serverAddress, serverPort); 
        UDPsocket.send(packet);
    } 
    **/
    // DEBUG: simulate packet loss (30% drop)
    private static final double DROP_RATE = 0.3;

    private void sendMessage(Message message) throws IOException {
        if (Math.random() < DROP_RATE) {
            System.out.println("[DEBUG] Simulated packet drop: " + message.msgTypeString() +
                            " Seq=" + message.getSequenceNum());
            return; // pretend packet was sent
        }

        byte[] data = message.convertToBytes();
        data = Cryptography.encrypt(data);
        DatagramPacket packet = new DatagramPacket(data, data.length, serverAddress, serverPort);
        UDPsocket.send(packet);
    }

    /**
     * Blocks until a UDP datagram arrives from the server, then parses it.
     * Throws SocketTimeoutException if no packet arrives within TIMEOUT ms.
     */
    private Message receiveMessage() throws IOException
    {
        byte[] buffer = new byte[BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        UDPsocket.receive(packet);
        byte[] decrypted = Cryptography.decrypt(Arrays.copyOf(packet.getData(), packet.getLength()));
        
        return Message.convertToMessage(decrypted);
    }

    /**
     * Waits for an ACK with the specified SeqNum.
     * Returns true if the expected ACK is received, false on wrong ACK or timeout.
     */
    private boolean waitAck(int expectedSeq) throws IOException
    {
        try 
        {
            Message response = receiveMessage();

            if(response.getMessageType() == Message.ACK && response.getSequenceNum() == expectedSeq)
            {
                System.out.println("Received ACK for SeqNum = " + expectedSeq);
                return true;
            }
            else
            {
                System.out.println(Colors.yellow("[CLIENT] Wrong ACK: Expected SeqNum=" + expectedSeq + ", Received SeqNum =" + response.getSequenceNum()));
            }
        }
        catch (SocketTimeoutException e)
        {
            System.out.println("[CLIENT] Timeout waiting for ACK SeqNum = " + expectedSeq);
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

    /**
     * Writes a list of byte chunks to a file in order.
     * Used to reassemble a received file after all chunks are collected.
     */
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

    /**
     * Downloads a file from the server and saves it locally.
     *
     * Protocol flow:
     *   1. Client sends DATA(sequenceNum, filename) as a download request.
     *   2. Server responds with DATA packets (stop-and-wait).
     *   3. Client ACKs each in-order packet; sends duplicate ACK for out-of-order.
     *   4. Server sends FIN when all data is sent; client responds with FIN_ACK.
     *   5. Client reassembles chunks and writes the file.
     *
     * @param remoteFilename  filename to request from the server
     * @param localFilename   local path to save the downloaded file
     * @return true if the file was downloaded successfully, false otherwise
     */
    public boolean downloadFile(String remoteFilename, String localFilename) throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            System.out.println(Colors.red("Cannot start download. Connection is not established"));
            return false;
        }

        System.out.println(Colors.cyan("\n===== Starting File Download ====="));
        System.out.println("Remote: " + remoteFilename + "  ->  Local: " + localFilename);

        // send read request to server
        Message readRequest = new Message(Message.DATA, sequenceNum, remoteFilename.getBytes());
        System.out.println("\nSending read request...");
        sendMessage(readRequest);
        System.out.println("[CLIENT] Sent download request. Expected SeqNum = " + sequenceNum + ", File = '" + remoteFilename + "'");
        sequenceNum++;

        List<byte[]> receivedChunks = new ArrayList<>();
        int expectedSeq = sequenceNum - 1;
        boolean complete = false;

        while(!complete)
        {
            try 
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.ERROR)
                {
                    String error = new String(response.getPayload());
                    System.out.println("Received server error message: " + Colors.red(error));
                    return false;
                }
                else if(response.getMessageType() == Message.DATA)
                {
                    int recvSeqNum = response.getSequenceNum();
                    int recvPayloadLen = response.getPayloadLen();
                    System.out.println("Received DATA packet. SeqNum = " + recvSeqNum + ", Payload Length = " + recvPayloadLen);

                    if(recvSeqNum == expectedSeq)
                    {
                        receivedChunks.add(response.getPayload());
                        Message ack = new Message(Message.ACK, recvSeqNum);
                        sendMessage(ack);
                        System.out.println("[CLIENT] Message sent [Type = ACK, SeqNum = " + recvSeqNum + "]");
                        expectedSeq++;
                    }
                    else 
                    {
                        System.out.println(Colors.yellow("Out of order packet. Expected SeqNum = " + expectedSeq + ", Received SeqNum = " + recvSeqNum));
                            
                        // if packet is duplicate
                        if(expectedSeq > sequenceNum - 1)
                        {
                            Message dupeAck = new Message(Message.ACK, expectedSeq - 1);
                            sendMessage(dupeAck);
                            System.out.println("[CLIENT] Sent duplicate ACK for SeqNum = " + (expectedSeq - 1));
                        }
                    }
                }
                else if(response.getMessageType() == Message.FIN)
                {
                    System.out.println("Received FIN for SeqNum = " + response.getSequenceNum());

                    Message fin_ack = new Message(Message.FIN_ACK, response.getSequenceNum());
                    sendMessage(fin_ack);
                    System.out.println("[CLIENT] Message sent [Type = FIN_ACK, SeqNum = "+ response.getSequenceNum() + "]");
                    complete = true;
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println(Colors.yellow("Timeout waiting for data."));

                // request retransmission
                // System.out.println("Request Retransmission");
                break;
            }
        }

        if(complete)
        {
            writeChunksToFile(receivedChunks, localFilename);
            System.out.println("File Downloaded Successfully -> '" + localFilename + "'");
            System.out.println("Total packet received: " + receivedChunks.size());
            System.out.println(Colors.green("FILE DOWNLOAD COMPLETE"));

            return true;
        }

        return false; 
    }

    public boolean uploadFile(String localFilename, String remoteFilename) throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            System.out.println(Colors.red("Cannot upload file. Connection is not established"));
            return false;
        }

        System.out.println(Colors.cyan("\n===== Starting File Upload ====="));
        System.out.println("Local: " + localFilename + "  ->   Remote: " + remoteFilename);

        File file = new File(localFilename);
        if(!file.exists())
        {
            System.out.println(Colors.red("Error: File not found."));
            return false;
        }

        Message filename = new Message(Message.DATA, sequenceNum, remoteFilename.getBytes());
        sendMessage(filename);
        System.out.println("[CLIENT] Sent filename: SeqNum = " + sequenceNum);

        if(!waitAck(sequenceNum))
        {
            System.out.println(Colors.red("[CLIENT] No ACK received for filename. Upload aborted."));
            return false;
        }
        sequenceNum++;

        FileInputStream fis = null;

        try {
            fis = new FileInputStream(file);
            byte[] buffer = new byte[MAX_PAYLOAD_SIZE];
            int bytesRead;

            while ((bytesRead = fis.read(buffer)) != -1) {
                byte[] payload = Arrays.copyOf(buffer, bytesRead);  
                Message data = new Message(Message.DATA, sequenceNum, payload);
                boolean acked = false;

                for (int i = 0; i < MAX_RETRIES && !acked; i++) {
                    sendMessage(data);
                    System.out.println("[CLIENT] Message sent [Type = DATA, SeqNum = " + sequenceNum +
                                    ", Payload = " + bytesRead + " bytes]");

                    if (waitAck(sequenceNum)) {
                        acked = true;
                    } else {
                        System.out.println("[CLIENT] Retrying SeqNum = " + sequenceNum +
                                        " (attempt " + (i + 1) + "/" + MAX_RETRIES + ")");
                    }
                }

                if (!acked) {
                    System.out.println(Colors.red("[CLIENT] Failed to send DATA SeqNum = "
                                                + sequenceNum + ". Upload aborted"));
                    return false;
                }

                sequenceNum++;  
            }

            Message fin = new Message(Message.FIN, sequenceNum);
            boolean complete = false;
            for (int i = 0; i < MAX_RETRIES && !complete; i++) {
                sendMessage(fin);
                System.out.println("[CLIENT] Message sent [Type = FIN, SeqNum = " + sequenceNum + "]");

                try {
                    Message response = receiveMessage();
                    if (response.getMessageType() == Message.FIN_ACK) {
                        System.out.println("Received FIN_ACK for SeqNum = " + response.getSequenceNum());
                        complete = true;
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println("Timeout waiting for FIN_ACK, retrying (" +
                                    (i + 1) + "/" + MAX_RETRIES + ")");
                }
            }

            if (complete) {
                System.out.println(Colors.green("File Uploaded Successfully -> '" + remoteFilename + "'"));
                System.out.println(Colors.green("FILE UPLOADED SUCCESSFULLY"));
                return true;
            } else {
                System.out.println(Colors.red("Upload FIN not acknowledged. Transfer may be incomplete."));
                return false;
            }

        } finally {
            if (fis != null) fis.close();
        }

    }

    /**
     * Performs the session teardown by sending FIN and waiting for FIN_ACK.
     * Uses sequenceNum (the connection-level counter) for the FIN message.
     * If no FIN_ACK is received after MAX_RETRIES, forces the session closed.
     */
    public void disconnect() throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            return;
        }

        System.out.println(Colors.cyan("\n====== Terminating Connection ======"));

        Message fin = new Message(Message.FIN, sequenceNum);
        sendMessage(fin);
        state = ClientState.FIN_WAIT;
        System.out.println("[CLIENT] Message sent [Type = FIN, SeqNum = " + sequenceNum + "]");

        for(int i = 0; i < MAX_RETRIES; i++)
        {
            try 
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.FIN_ACK)
                {
                    System.out.println("Received FIN_ACK for SeqNum = " + response.getSequenceNum());
                    state = ClientState.CLOSED;
                    System.out.println(Colors.green("CONNECTION CLOSED"));
                    return;
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println("Timeout waiting for ACK, retrying (" + (i + 1) + "/" + MAX_RETRIES + ")");
                sendMessage(fin); // retransmit FIN message
            }
        }

        state = ClientState.CLOSED;
        System.out.println(Colors.red("Forced disconnect after timeout."));
    }

     /** Closes the UDP socket and releases system resources. */
    public void close()
    {
        if(UDPsocket != null && !UDPsocket.isClosed())
        {
            UDPsocket.close();
        }
    }

    public static void main(String[] args) {

        System.out.println(Colors.cyan("===== Starting CLIENT program ====="));
        Scanner scanner = new Scanner(System.in);
        Client client = null;

        int clientPort = 0, serverPort = 0;

        while (true) {

            System.out.print("Enter client port: ");

            try {

                clientPort = Integer.parseInt(scanner.nextLine().trim());

                if (clientPort <= 0 || clientPort > 65535) {
                    System.out.println(Colors.red("Invalid port number. Please enter 1–65535."));
                    continue;
                }

                break;

            } catch (NumberFormatException e) {

                System.out.println(Colors.red("Invalid input. Please enter a valid number for port."));

            }
        }

        try {

            client = new Client(clientPort);

        } catch (Exception e) {

            System.out.println(Colors.red("Failed to initialize client socket: " + e.getMessage()));
            return;

        }

        System.out.print("Enter server host (localhost): ");
        String serverHost = scanner.nextLine().trim();

        if (serverHost.isEmpty()) serverHost = "localhost";

        while (true) {

            System.out.print("Enter server port: ");
            try {

                serverPort = Integer.parseInt(scanner.nextLine().trim());
                if (serverPort <= 0 || serverPort > 65535) {

                    System.out.println(Colors.red("Invalid port number. Please enter 1–65535."));
                    continue;
                }
                break;

            } catch (NumberFormatException e) {

                System.out.println(Colors.red("Invalid input. Please enter a valid number for port."));
            }
        }

        try {
            if (!client.connect(serverHost, serverPort)) {

                System.out.println(Colors.red("Failed to establish connection to server."));
                return;
            }

            boolean flag = false;

            while (!flag) {

                System.out.println(Colors.cyan("\n===== File Transfer Functionality ====="));
                System.out.println("[1] Download File");
                System.out.println("[2] Upload File");
                System.out.println("[X] Disconnect");
                System.out.print("Choose option: ");

                String choice = scanner.nextLine().trim();

                switch (choice) {
                    case "1":

                        System.out.println(Colors.cyan("\n===== Download File ====="));
                        System.out.print("Enter remote filename (on server): ");
                        String remoteFile = scanner.nextLine().trim();
                        System.out.print("Enter local filename to save as: ");
                        String localFile = scanner.nextLine().trim();

                        if (!remoteFile.isEmpty() && !localFile.isEmpty())
                            client.downloadFile(remoteFile, localFile);

                        else
                            System.out.println(Colors.red("Filename cannot be empty."));

                        break;

                    case "2":

                        System.out.println(Colors.cyan("\n===== Upload File ====="));
                        System.out.print("Enter local filename to upload: ");
                        String localUpload = scanner.nextLine().trim();
                        System.out.print("Enter remote filename to save as: ");
                        String remoteUpload = scanner.nextLine().trim();

                        if (!localUpload.isEmpty() && !remoteUpload.isEmpty())
                            client.uploadFile(localUpload, remoteUpload);

                        else
                            System.out.println(Colors.red("Filename cannot be empty."));

                        break;

                    case "X":
                    case "x":

                        flag = true;
                        break;
                    default:

                        System.out.println(Colors.yellow("Invalid option. Please choose 1, 2, or X."));
                }
            }

            client.disconnect();

        } catch (UnknownHostException e) {

        System.out.println(Colors.red("Invalid server hostname: " + e.getMessage()));
        System.out.println(Colors.yellow("Please restart the client and enter a valid host (e.g., localhost or 127.0.0.1)."));
    
    } catch (Exception e) {

        System.out.println(Colors.red("Error: " + e.getMessage()));
    
    } finally {

        if (client != null) client.close();
        scanner.close();

        }
}


    /** fields */
    private DatagramSocket UDPsocket;
    private InetAddress serverAddress;
    private ClientState state;
    private int serverPort;
    private int sequenceNum;
    private int expectedAckNum;

    /** configuration constants */
    private static final int TIMEOUT = 5000; // 5 seconds
    private static final int MAX_RETRIES = 3;
    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_PAYLOAD_SIZE = 1000;
}