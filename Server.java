import java.io.*;
import java.net.*;
import java.util.*;

/**
 *  The class Server represents the server in the reliable data transfer protocol over UDP.
 * 
 *  All files managed by the server are stored inside a dedicated SERVER_FOLDER
 *  directory that is created automatically on first run. This makes it easy to verify
 *  that uploads and downloads are reaching the correct endpoint even when both client
 *  and server run on the same machine.
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

    // Server instance variables
    private DatagramSocket UDPsocket;
    private InetAddress clientAddress;
    private int clientPort;
    private ServerState state;
    private int clientSeqBase;

    // Protocol parameters
    private static final int TIMEOUT = 5000; // 5 seconds
    private static final int MAX_RETRIES = 3;
    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_PAYLOAD_SIZE = 1000;

    // Directory where the server stores files for upload/download operations
    private static final String SERVER_FOLDER = "ServerFolder";

    // Sabotage mode parameters for testing robustness under lossy conditions
    private static boolean sabotageMode = false;
    private static double serverDropRate = 0.2;
    private static int serverDelayMs = 500;

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

        File folder = new File(SERVER_FOLDER);
        if (!folder.exists())
        {
            folder.mkdirs();
            System.out.println("[SERVER] Created directory: " + SERVER_FOLDER + "/");
        }

        System.out.println("[SERVER] Listening on port: " + serverPort + " | File Directory = " + SERVER_FOLDER + "/");
    }

    /**
     * Serializes, encrypts, and sends a Message to the current client.
     *
     * @param message  the Message to transmit
     * @throws IOException if the underlying socket send fails
     */
    private void sendMessage(Message message) throws IOException
    {
        if(sabotageMode)
        {
            if(Math.random() < serverDropRate)
            {
                System.out.println(Colors.yellow("[SERVER SABOTAGE] Dropped " + message.msgTypeString() + " SeqNum = " + message.getSequenceNum()));
                return;
            }

            if(serverDelayMs > 0 && Math.random() < 0.3)
            {
                try
                {
                    Thread.sleep(serverDelayMs);
                    System.out.println(Colors.yellow("[SERVER SABOTAGE] Delayed " + message.msgTypeString() + " SeqNum = " + message.getSequenceNum()));
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        byte[] data = message.convertToBytes();
        data = Cryptography.encrypt(data);
        DatagramPacket packet = new DatagramPacket(data, data.length, clientAddress, clientPort);
        UDPsocket.send(packet);
    }

    /**
     * Blocks until a UDP datagram arrives, then decrypts and parses it into a
     * Message. Also records the sender's address and port so that
     * sendMessage() can reply to the correct client.
     *
     * @return the received and decrypted Message
     * @throws IOException            if a network error occurs
     * @throws SocketTimeoutException if no packet arrives within TIMEOUT ms
     */
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
     * 
     * @throws IOException if an unrecoverable network error occurs
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
                    System.out.println("         Max retries : " + MAX_RETRIES + "\n");

                } else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.DATA) {

                    String payload = new String(msg.getPayload()).trim();

                    if (payload.equals("LIST")) 
                    {
                        System.out.println(Colors.cyan("\n====== Sending ServerFolder File List====="));
                        String[] files = listServerFiles();
                        String fileList = String.join("\n", files);
                        Message response = new Message(Message.DATA, msg.getSequenceNum(), fileList.getBytes());
                        sendMessage(response);
                        System.out.println("[SERVER] Sent file list to client.");
                    }

                } else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.READ) {

                    handleDownloadRequest(msg);

                } else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.WRITE) {

                    handleUploadRequest(msg);

                } else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.FIN) {

                    System.out.println(Colors.cyan("\n====== Terminating Connection ====="));
                    System.out.println("Received FIN, SeqNum = " + msg.getSequenceNum());
                    Message finAck = new Message(Message.FIN_ACK, msg.getSequenceNum());

                    sendMessage(finAck);

                    System.out.println("[SERVER] Message sent [Type = FIN_ACK, SeqNum = " + msg.getSequenceNum() + "]");
                    System.out.println(Colors.green("CONNECTION CLOSED WITH CLIENT."));

                    state = ServerState.LISTEN;

                    clientAddress = null;
                    clientPort = 0;
                    clientSeqBase = 0;

                    while (true) {
                        System.out.print(Colors.cyan("Do you want the server to keep listening for new connections? (Y/N): "));
                        String input = prompt.nextLine().trim().toUpperCase();

                        if ("Y".equals(input)) {
                            System.out.println(Colors.green("Server will continue listening on port."));
                            break;
                        } else if ("N".equals(input)) {
                            System.out.println(Colors.red("Server shutting down..."));
                            UDPsocket.close();
                            return;
                        } else {
                            System.out.println(Colors.yellow("Invalid choice. Please enter Y or N."));
                        }
                    }
                }
            } catch (Exception e) {

                System.out.println(Colors.red("[SERVER] Error handling message: " + e.getMessage()));
                e.printStackTrace();

            }
        }
    }

    /**
     * Returns the list of filenames currently stored in the server's folder.
     *
     * The result is used to answer client LIST requests and to drive server-
     * side debugging or manual inspection of available files.
     *
     * @return array of simple filenames under {@value #SERVER_FOLDER}, or an
     *         empty array if the directory is empty or cannot be listed
     */
    private String[] listServerFiles()
    {
        File folder = new File(SERVER_FOLDER);
        String[] files = folder.list();

        if(files == null || files.length == 0)
        {
            return new String[0];
        }

        return files;
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
        
        int requestSeq = request.getSequenceNum();
        System.out.println("[SERVER] Received READ request for: '" + filename + "' SeqNum = " + requestSeq);

        File file = new File(SERVER_FOLDER, filename);

        if (!file.exists())
        {
            Message error = new Message(Message.ERROR, request.getSequenceNum(),
                    ("File not found: " + filename).getBytes());
            sendMessage(error);

            System.out.println(Colors.red("[SERVER] File not found: " + filename));
            return;
        }

        // ACK the download request before sending any data
        Message requestAck = new Message(Message.ACK, requestSeq);
        sendMessage(requestAck);
        System.out.println("[SERVER] Message sent [Type = ACK, SeqNum = " + requestSeq + "]");

        long fileSize = file.length();
        System.out.println("[SERVER] Sending: '" + filename + "' (" + fileSize + " bytes)");

        FileInputStream fis = null;

        try 
        {
            fis = new FileInputStream(file);
            byte[] buffer = new byte[MAX_PAYLOAD_SIZE];
            int bytesRead;
            int seq = requestSeq + 1;

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
                            System.out.println(Colors.yellow("Wrong ACK received. Expected SeqNum = " + seq + ", Received SeqNum = " + ack.getSequenceNum()));
                        }
                    }
                    catch (SocketTimeoutException e)
                    {
                        System.out.println(Colors.yellow("[SERVER] Timeout waiting for ACK SeqNum = " + seq + " (attempt " + (i + 1) + "/" + MAX_RETRIES + ")"));
                    }
                    catch (SecurityException | IllegalArgumentException e)
                    {
                        System.out.println(Colors.red("[SERVER] Dropped corrupted or malformed ACK for SeqNum = " + seq + ": " + e.getMessage()));
                    }
                }

                if (!acked)
                {
                    System.out.println(Colors.red("[SERVER] Transfer failed. No ACK for SeqNum = "
                            + seq + " after " + MAX_RETRIES + " attempts."));

                    Message error = new Message(Message.ERROR, seq,
                            ("Transfer aborted: no ACK for SeqNum = " + seq).getBytes());
                    sendMessage(error);

                    return;
                }

                seq++;
            }

            Message fin = new Message(Message.FIN, seq);
            boolean finAcked = false;

            for (int i = 0; i < MAX_RETRIES && !finAcked; i++) {
                sendMessage(fin);
                System.out.println("[SERVER] Message sent [Type = FIN, SeqNum = "+ seq + "]");

                try {
                    Message response = receiveMessage();
                    if (response.getMessageType() == Message.FIN_ACK) {
                        System.out.println("Received FIN_ACK for SeqNum = " + response.getSequenceNum());
                        finAcked = true;
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println(Colors.yellow("Timeout waiting for FIN_ACK (attempt " + (i + 1) + "/" + MAX_RETRIES + ")"));
                } catch (SecurityException | IllegalArgumentException e) {
                    System.out.println(Colors.red("[SERVER] Corrupted or malformed FIN_ACK during download FIN: " + e.getMessage()));
                }
            }

            if (finAcked) {
                System.out.println(Colors.green("[SERVER] Download complete for '" + filename + "'"));
            } else {
                System.out.println(Colors.red("[SERVER] Download may be incomplete - FIN not acknowledged after " + MAX_RETRIES + " attempts"));
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

    /**
     * Handles a file upload from the client and stores it in SERVER_FOLDER.
     *
     * Protocol flow:
     * 1. Request message payload must begin with "UPLOAD:" followed by the target filename.
     * 2. Server sends ACK for the filename message.
     * 3. Server receives DATA chunks in order, writing each to disk and
     *    sending ACK; duplicate / out-of-order chunks get a duplicate ACK.
     * 4. On receipt of FIN the server sends FIN_ACK and closes the file.
     *
     * @param request  the DATA message carrying "UPLOAD:<filename>"
     * @throws IOException if a network or file-system error occurs
     */
    private void handleUploadRequest(Message request) throws IOException
    {
        System.out.println(Colors.cyan("\n===== Handling Upload Request ====="));

        String filename = new String(request.getPayload()).trim();
        int requestSeq = request.getSequenceNum();
        System.out.println("Received WRITE request for: '" + filename + "' SeqNum = " + requestSeq);

        File file = new File(SERVER_FOLDER, filename);
        System.out.println("[SERVER] Will save to: " + file.getPath() + "\n");

        // ACK the upload request and wait for the first DATA packet as confirmation
        Message requestAck = new Message(Message.ACK, requestSeq);
        Message firstData = null;

        for(int i = 0; i < MAX_RETRIES && firstData == null; i++)
        {
            sendMessage(requestAck);
            System.out.println("[SERVER] Message sent [Type = ACK, SeqNum = " + requestSeq + "]");

            try
            {
                Message incoming = receiveMessage();

                if(incoming.getMessageType() == Message.DATA && incoming.getSequenceNum() == requestSeq + 1)
                {
                    System.out.println("[SERVER] Client confirmed upload. First DATA SeqNum = " + incoming.getSequenceNum());
                    firstData = incoming; // save it so we don't lose the first chunk
                }
                else if(incoming.getMessageType() == Message.DATA)
                {
                    System.out.println(Colors.yellow("[SERVER] Unexpected SeqNum on first DATA. Expected "
                            + (requestSeq + 1) + ", got " + incoming.getSequenceNum()));
                }
                else
                {
                    System.out.println(Colors.yellow("[SERVER] Unexpected message during upload handshake: "
                            + incoming.msgTypeString()));
                }
            }
            catch(SocketTimeoutException e)
            {
                System.out.println(Colors.yellow("[SERVER] Timeout waiting for first DATA packet (attempt "
                        + (i + 1) + "/" + MAX_RETRIES + ")"));
            }
            catch(SecurityException | IllegalArgumentException e)
            {
                System.out.println(Colors.red("Corrupted first DATA during upload handshake: " + e.getMessage()));
            }
        }

        if(firstData == null)
        {
            System.out.println(Colors.red("Client did not begin upload. Aborting."));
            return;
        }
        
        FileOutputStream fos = null;

        try {
            fos = new FileOutputStream(file);
            int expectedSeq = requestSeq + 1;
            boolean receiving = true;

            fos.write(firstData.getPayload());
            fos.flush();
            System.out.println("Received DATA SeqNum = " + firstData.getSequenceNum()
                    + " (" + firstData.getPayload().length + " bytes)");
            Message ackFirst = new Message(Message.ACK, firstData.getSequenceNum());
            sendMessage(ackFirst);
            System.out.println("[SERVER] Message sent [Type = ACK, SeqNum = " + firstData.getSequenceNum() + "]");
            expectedSeq++;

            int uploadTimeouts = 0;
            while (receiving) {
                try {
                    Message msg = receiveMessage();
                    uploadTimeouts = 0; // reset on any successful receive
                    if (msg.getMessageType() == Message.DATA) {
                        if (msg.getSequenceNum() == expectedSeq) {
                            fos.write(msg.getPayload());
                            fos.flush();
                            System.out.println("Received DATA SeqNum = " + msg.getSequenceNum()
                                    + " (" + msg.getPayload().length + " bytes)");
                            Message ackData = new Message(Message.ACK, msg.getSequenceNum());
                            sendMessage(ackData);
                            System.out.println("[SERVER] Message sent [Type = ACK, SeqNum = "+ msg.getSequenceNum() + "]");
                            expectedSeq++;
                        } else {
                            // If out of order, resend last ACK
                            System.out.println(Colors.yellow("[SERVER] Out of order. Expected SeqNum = " + expectedSeq));
                            Message dupAck = new Message(Message.ACK, expectedSeq - 1);
                            sendMessage(dupAck);
                        }
                    }
                    else if (msg.getMessageType() == Message.FIN) {
                        System.out.println("Received FIN for SeqNum = " + msg.getSequenceNum());
                        Message finAck = new Message(Message.FIN_ACK, msg.getSequenceNum());
                        sendMessage(finAck);
                        System.out.println("[SERVER] Message sent [Type = FIN_ACK, SeqNum = "+ msg.getSequenceNum() + "]");
                        receiving = false;
                        System.out.println(Colors.green("[SERVER] Upload complete for '" + file.getPath() + "'"));
                    }
                }
                catch (SocketTimeoutException e) {
                    uploadTimeouts++;
                    System.out.println(Colors.yellow("[SERVER] Timeout waiting for upload data (attempt "
                            + uploadTimeouts + "/" + MAX_RETRIES + ")"));
                    if (uploadTimeouts >= MAX_RETRIES)
                    {
                        System.out.println(Colors.red("[SERVER] Upload aborted: client unresponsive after "
                                + MAX_RETRIES + " timeouts."));
                        receiving = false;
                    }
                }
                catch (SecurityException | IllegalArgumentException e) {
                    System.out.println(Colors.red("[SERVER] Dropped corrupted or malformed upload packet: " + e.getMessage()));
                }
            }
        }
        finally {
            if (fos != null) fos.close();
        }
    }


    /**
     * Closes the underlying UDP socket and releases all system resources.
     * Safe to call even if the socket is already closed or was never opened.
     */
    public void close()
    {
        if (UDPsocket != null && !UDPsocket.isClosed())
            UDPsocket.close();
    }

    /**
     * Optionally enables server-side sabotage mode for testing.
     *
     * When the user answers 'y', outgoing packets from the server are randomly
     * dropped or delayed according to {@code serverDropRate} and
     * {@code serverDelayMs}. This is used to demonstrate the robustness of the
     * stop-and-wait protocol under lossy conditions.
     *
     * @param scanner interactive console scanner used to read the user's choice
     */
    public static void configureSabotageMode(Scanner scanner)
    {
        System.out.print("Enable server sabotage mode for testing? (y/n): ");    
        String choice = scanner.nextLine().trim().toLowerCase();    

        if(choice.startsWith("y"))
        {
            sabotageMode = true;
            System.out.println(Colors.yellow("[SERVER SABOTAGE] Enabled with 20% drop rate and 500ms delay"));
        }
    }

    /**
     * Application entry point. Prompts for a port number, then starts the
     * server loop.  Cleans up the socket in a finally block.
     *
     * @param args  command-line arguments (not used)
     */
    public static void main(String[] args)
    {
        System.out.println(Colors.cyan("===== Starting SERVER program ====="));
        Scanner scanner = new Scanner(System.in);
        int port = 0;
        
        while (true) {

            System.out.print("Enter server port: ");

            try 
            {
                port = Integer.parseInt(scanner.nextLine().trim());
                if (port <= 0 || port > 65535) 
                {
                    System.out.println(Colors.red("Invalid port number. Please enter a value between 1-65535."));
                    continue;
                }
                break;
            } 
            catch (NumberFormatException e) 
            {
                System.out.println(Colors.red("Invalid input. Please enter a valid number for port."));
            }
        }

        Server server = null;
        try 
        {
            server = new Server(port);
            server.start();
        } 
        catch (Exception e) 
        {
            System.out.println(Colors.red("Error: " + e.getMessage()));
            e.printStackTrace();
        } 
        finally 
        {
            if (server != null) server.close();
            scanner.close();
        }
    }
}