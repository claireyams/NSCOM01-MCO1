import java.io.*;
import java.net.*;
import java.util.*;

//last update: Feb 21, 2026 10:05am

/**
 *  The class Server represents the server in the reliable data transfer protocol over UDP.
 * 
 *  @author Sky Hannah Parado
 *  @author Rhian Claire Yamsuan
 *  @version 1.0
 */
public class Server
{
    /**
     * Server states as defined by the protocol state machine.
     *
     *  LISTEN       — waiting for a new client SYN
     *  SYN_RECEIVED — SYN_ACK sent, waiting for client ACK
     *  ESTABLISHED  — session active, ready for file operations
     *  CLOSE_WAIT   — FIN received, sending FIN_ACK, returning to LISTEN
     */
    private enum ServerState
    {
        LISTEN, SYN_RECEIVED, ESTABLISHED, CLOSE_WAIT
    }

    private DatagramSocket UDPsocket;
    private InetAddress clientAddress;
    private int clientPort;
    private ServerState state;
    private int clientSeqBase;

    private static final int TIMEOUT = 5000; // 5 seconds
    private static final int MAX_RETRIES = 3;
    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_PAYLOAD_SIZE = 1000;

    /**
     * Creates a Server bound to the given port.
     * @param serverPort  UDP port to listen on
     * @throws SocketException if the socket cannot be created
     */
    public Server(int serverPort) throws SocketException
    {
        UDPsocket = new DatagramSocket(serverPort);
        UDPsocket.setSoTimeout(TIMEOUT);
        state = ServerState.LISTEN;

        System.out.println("Server listening on port: " + serverPort);
    }

    private void sendMessage(Message message) throws IOException
    {
        byte[] data = message.convertToBytes();
        data = Cryptography.encrypt(data);
        DatagramPacket packet = new DatagramPacket(data, data.length, clientAddress, clientPort);
        UDPsocket.send(packet);
    }

    private Message receiveMessage() throws IOException
    {
        byte[] buffer = new byte[BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        UDPsocket.receive(packet);

        clientAddress = packet.getAddress();
        clientPort = packet.getPort();

        byte[] decrypted = Cryptography.decrypt(Arrays.copyOf(packet.getData(), packet.getLength()));
        return Message.convertToMessage(decrypted);
    }

    /**
     * Starts the server's main event loop.
     * The server runs indefinitely, accepting one client at a time.
     * After a session ends, it returns to LISTEN state for the next client.
     */
    public void start() throws IOException {

        int waitCounter = 0;
        Scanner prompt = new Scanner(System.in);

        while (true) {

            Message msg = null;

            try {

                msg = receiveMessage();
                waitCounter = 0;

            } catch (SocketTimeoutException e) {

                waitCounter++;
                if (waitCounter % 6 == 1) System.out.println("[SERVER] Waiting for client...");
                continue;

            } catch (SecurityException e) {

                System.out.println(Colors.red("[SERVER] Dropped corrupted packet: " + e.getMessage()));
                continue;

            } catch (IllegalArgumentException e) {

                System.out.println(Colors.red("[SERVER] Received malformed message: " + e.getMessage()));
                continue;

            } catch (Exception e) {

                System.out.println(Colors.red("[SERVER] Unexpected error reading packet: " + e.getMessage()));
                e.printStackTrace();
                continue;

            }
            try {
                if (state == ServerState.LISTEN && msg.getMessageType() == Message.SYN) {

                    clientSeqBase = msg.getSequenceNum();

                    System.out.println("Received SYN, SeqNum = " + clientSeqBase);
                    Message synAck = new Message(Message.SYN_ACK, clientSeqBase);

                    sendMessage(synAck);

                    System.out.println("[SERVER] Message sent [Type = SYN_ACK, SeqNum = " + clientSeqBase + "]");
                    state = ServerState.SYN_RECEIVED;

                } else if (state == ServerState.SYN_RECEIVED && msg.getMessageType() == Message.ACK) {

                    System.out.println("Received ACK, SeqNum = " + msg.getSequenceNum());
                    state = ServerState.ESTABLISHED;

                    System.out.println(Colors.green("CONNECTION ESTABLISHED"));
                    System.out.println("\n[SERVER] Session parameters:");
                    System.out.println("         Client ISN  : " + clientSeqBase);
                    System.out.println("         Max payload : " + MAX_PAYLOAD_SIZE + " bytes");
                    System.out.println("         Timeout     : " + TIMEOUT + " ms");
                    System.out.println("         Max retries : " + MAX_RETRIES);

                } else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.DATA) {

                    String payload = new String(msg.getPayload()).trim();
                    File f = new File(payload);

                    if (f.exists()) {
                        handleDownloadRequest(msg);

                    } else {

                        Message uploadStart = new Message(Message.DATA, msg.getSequenceNum(),
                                ("UPLOAD:" + payload).getBytes());
                        handleUploadRequest(uploadStart);

                    }

                } else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.FIN) {

                    System.out.println("Received FIN, SeqNum = " + msg.getSequenceNum());
                    Message finAck = new Message(Message.FIN_ACK, msg.getSequenceNum());

                    sendMessage(finAck);

                    System.out.println("[SERVER] Message sent [Type = FIN_ACK, SeqNum = " + msg.getSequenceNum() + "]");
                    System.out.println(Colors.green("CONNECTION CLOSED WITH CLIENT."));

                    state = ServerState.LISTEN;

                    clientAddress = null;
                    clientPort = 0;
                    clientSeqBase = 0;

                    System.out.print(Colors.cyan("Do you want the server to keep listening for new connections? (Y/N): "));
                    String input = prompt.nextLine().trim().toUpperCase();

                    if (input.equals("N")) {

                        System.out.println(Colors.red("Server shutting down..."));
                        UDPsocket.close();
                        break;

                    } else {

                        System.out.println(Colors.green("Server will continue listening on port."));
                    }
                }
            } catch (Exception e) {

                System.out.println(Colors.red("[SERVER] Error handling message: " + e.getMessage()));
                e.printStackTrace();

            }
        }
    }

    /**
     * Handles a client download request.
     *
     * Protocol flow:
     *   1. Client sends DATA with filename as payload.
     *   2. Server checks the file exists; sends ERROR if not.
     *   3. Server sends file data in MAX_PAYLOAD_SIZE chunks, stop-and-wait.
     *   4. Server sends FIN when all chunks are sent; waits for FIN_ACK.
     *
     * Sequence numbers for data packets are based on the client's initial
     * sequence number (clientSeqBase) to keep a unified sequence space.
     */
    private void handleDownloadRequest(Message request) throws IOException
    {
        System.out.println(Colors.cyan("\n===== Handling Download Request ====="));

        String filename = new String(request.getPayload()).trim();
        File file = new File(filename);

        if (!file.exists())
        {
            Message error = new Message(Message.ERROR, request.getSequenceNum(),
                    ("File not found: " + filename).getBytes());
            sendMessage(error);

            System.out.println(Colors.red("[SERVER] File not found: " + filename));
            return;
        }

        long fileSize = file.length();
        System.out.println("[SERVER] Sending: '" + filename + "' (" + fileSize + " bytes)");

        FileInputStream fis = null;

        try 
        {
            fis = new FileInputStream(file);
            byte[] buffer = new byte[MAX_PAYLOAD_SIZE];
            int bytesRead;
            int seq = request.getSequenceNum();

            
            while ((bytesRead = fis.read(buffer)) != -1)
            {
                byte[] payload = Arrays.copyOf(buffer, bytesRead);
                Message data = new Message(Message.DATA, seq, payload);
                boolean acked = false;

                for (int i = 0; i < MAX_RETRIES && !acked; i++)
                {
                    sendMessage(data);
                    System.out.println("[SERVER] Message sent [Type = DATA, SeqNum = " + seq + ", Payload = " + bytesRead + "]");

                    try
                    {
                        Message ack = receiveMessage();

                        if (ack.getMessageType() == Message.ACK && ack.getSequenceNum() == seq)
                        {
                            System.out.println("Received ACK for SeqNum = " + seq);
                            acked = true;
                        }
                        else
                        {
                            System.out.println(Colors.yellow("Wrong ACK received. Expected SeqNum = " + seq + "Received SeqNum = " + ack.getSequenceNum()));
                        }
                    }
                    catch (SocketTimeoutException e)
                    {
                        System.out.println(Colors.yellow("[SERVER] Timeout waiting for ACK SeqNum = " + seq + " (attempt " + (i + 1) + "/" + MAX_RETRIES + ")"));
                    }
                }

                if (!acked)
                {
                    System.out.println(Colors.red("[SERVER] Transfer failed. No ACK for SeqNum = " + seq + " after " + MAX_RETRIES + " attempts."));
                    fis.close();
                    return;
                }

                seq++;
            }

            Message fin = new Message(Message.FIN, seq);
            sendMessage(fin);
            System.out.println("[SERVER] Message sent [Type = FIN, SeqNum = "+ seq + "]");

            Message response = receiveMessage();
            if(response.getMessageType() == Message.FIN_ACK)
            {
                System.out.println("Received FIN_ACK for SeqNum = " + response.getSequenceNum());
            }
        }
        finally
        {
            if (fis != null)
            {
                fis.close();
            }
        }
    }

    private void handleUploadRequest(Message request) throws IOException
{
    System.out.println(Colors.cyan("\n===== Handling Upload Request ====="));
    
    String payload = new String(request.getPayload()).trim();
    if (!payload.startsWith("UPLOAD:")) {
        Message error = new Message(Message.ERROR, request.getSequenceNum(),
                "Invalid upload request format".getBytes());
        sendMessage(error);
        return;
    }
    String filename = payload.substring(7);
    File file = new File(filename);
    System.out.println("[SERVER] Preparing to receive file: " + filename);
    Message ack = new Message(Message.ACK, request.getSequenceNum());
    sendMessage(ack);
    System.out.println("[SERVER] ACK sent, ready to receive: SeqNum = " + request.getSequenceNum() + "]");
    FileOutputStream fos = null;
    try {
        fos = new FileOutputStream(file);
        int expectedSeq = request.getSequenceNum() + 1;
        boolean receiving = true;
        while (receiving) {
            try {
                Message msg = receiveMessage();
                if (msg.getMessageType() == Message.DATA) {
                    if (msg.getSequenceNum() == expectedSeq) {
                        fos.write(msg.getPayload());
                        fos.flush();
                        System.out.println("[SERVER] Received chunk SeqNum = " + msg.getSequenceNum()
                                + " (" + msg.getPayload().length + " bytes)");
                        Message ackData = new Message(Message.ACK, msg.getSequenceNum());
                        sendMessage(ackData);
                        expectedSeq++;
                    } else {
                        // If out of order, resend last ACK
                        System.out.println(Colors.yellow("[SERVER] Out of order. Expected SeqNum = " + expectedSeq));
                        Message dupAck = new Message(Message.ACK, expectedSeq - 1);
                        sendMessage(dupAck);
                    }
                }
                else if (msg.getMessageType() == Message.FIN) {
                    System.out.println("Received FIN, sending FIN_ACK...");
                    Message finAck = new Message(Message.FIN_ACK, msg.getSequenceNum());
                    sendMessage(finAck);
                    receiving = false;
                    System.out.println(Colors.green("[SERVER] Upload complete for '" + filename + "'"));
                }
            }
            catch (SocketTimeoutException e) {
                System.out.println(Colors.yellow("[SERVER] Waiting for more upload data..."));
            }
        }
    }
    finally {
        if (fos != null) fos.close();
    }
}

    public void close()
    {
        if (UDPsocket != null && !UDPsocket.isClosed())
            UDPsocket.close();
    }

    public static void main(String[] args) {

        System.out.println(Colors.cyan("===== Starting SERVER program ====="));
        Scanner scanner = new Scanner(System.in);
        int port = 0;
        
        while (true) {

            System.out.print("Enter server port: ");

            try {

                port = Integer.parseInt(scanner.nextLine().trim());

                if (port <= 0 || port > 65535) {
                    System.out.println(Colors.red("Invalid port number. Please enter a value between 1-65535."));
                    continue;
                }

                break;

            } catch (NumberFormatException e) {

                System.out.println(Colors.red("Invalid input. Please enter a valid number for port."));

            }
        }

        Server server = null;

        try {

            server = new Server(port);
            server.start();

        } catch (Exception e) {

            System.out.println(Colors.red("Error: " + e.getMessage()));
            e.printStackTrace();

        } finally {

            if (server != null) server.close();
            scanner.close();
            
        }
    }
}