import java.util.*;
import java.net.*;
import java.io.*;

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
     * Creates a Client bound to the given local port and ensures that
     * CLIENT_FOLDER exists on the local file system.
     *
     * The initial Sequence Number (ISN) starts at 0.
     *
     * @param clientPort  local UDP port to bind to
     * @throws SocketException if the DatagramSocket cannot be created
     */
    public Client(int clientPort) throws SocketException
    {
        UDPsocket = new DatagramSocket(clientPort);
        UDPsocket.setSoTimeout(TIMEOUT);
        state = ClientState.CLOSED;
        sequenceNum = 0;

        File folder = new File(CLIENT_FOLDER);
        if(!folder.exists())
        {
            folder.mkdirs();
            System.out.println("[CLIENT] Created directory: " + CLIENT_FOLDER + "/");
        }

        System.out.println("[CLIENT] Port Number = " + clientPort + " | File Directory = " + CLIENT_FOLDER + "/ |Initial SeqNum = " + sequenceNum);
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
     * @throws IOException if a network error occurs
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
            catch (SecurityException | IllegalArgumentException e)
            {
                System.out.println(Colors.red("[CLIENT] Corrupted or malformed handshake packet: " + e.getMessage()));
            }
        }

        System.out.println(Colors.red("Failed to establish connection after " + MAX_RETRIES + " attempts"));
        state = ClientState.CLOSED;

        return false;
    }

    
    private void sendMessage(Message message) throws IOException
    {
        if(sabotageMode)
        {
            // sequence blocking
            if(blockSequences && blockedSeqNum != -1 && message.getSequenceNum() == blockedSeqNum)
            {
                System.out.println(Colors.yellow("[SABOTAGE] Blocked transmission of " + message.msgTypeString() + " SeqNum = " + message.getSequenceNum() + " — retransmissions will be allowed (single packet loss simulation)"));
                blockedSeqNum = -1;
                return;
            }

            // artificial delay
            if(delayMs > 0) {
                try {
                    int actualDelay = (int)(delayMs * (0.5 + Math.random()));
                    System.out.println(Colors.yellow("[SABOTAGE] Applying " + actualDelay + "ms delay to " + message.msgTypeString() + " SeqNum = " + message.getSequenceNum()));
                    Thread.sleep(actualDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        byte[] data = message.convertToBytes();
        data = Cryptography.encrypt(data);
        DatagramPacket packet = new DatagramPacket(data, data.length, serverAddress, serverPort); 
        UDPsocket.send(packet);
    } 

    /**
     * Blocks until a UDP datagram arrives from the server, then parses it.
     * Throws SocketTimeoutException if no packet arrives within TIMEOUT ms.
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
        byte[] decrypted = Cryptography.decrypt(Arrays.copyOf(packet.getData(), packet.getLength()));
        
        return Message.convertToMessage(decrypted);
    }

    /**
     * Waits for a single ACK message with the expected sequence number.
     *
     * @param expectedSeq  the sequence number the ACK must carry
     * @return true if the correct ACK arrived; false otherwise
     * @throws IOException if a network error occurs (SocketTimeoutException is caught internally)
     */
    private boolean waitAck(int expectedSeq, int attempt, int maxRetries) throws IOException
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
                System.out.println(Colors.yellow("[CLIENT] Wrong ACK: Expected SeqNum = " + expectedSeq + ", Received SeqNum = " + response.getSequenceNum()));
            }
        }
        catch (SocketTimeoutException e)
        {
            System.out.println(Colors.yellow("[CLIENT] Timeout waiting for ACK SeqNum = " + expectedSeq
                    + " (" + attempt + "/" + maxRetries + ")"));
        }
        catch (SecurityException | IllegalArgumentException e)
        {
            System.out.println(Colors.red("[CLIENT] Dropped corrupted or malformed ACK for SeqNum = " + expectedSeq + ": " + e.getMessage()));
        }

        return false;
    }

    /**
     * Writes a list of byte chunks to a file in order.
     * Used to reassemble a received file after all chunks are collected.
     * 
     * @param chunks    ordered list of raw file data chunks received from the server
     * @param filename  bare filename to create inside the client folder
     * @throws IOException if the file cannot be written
     */
    private void writeChunksToFile(List<byte[]> chunks, String filename) throws IOException
    {
        File dest = new File(CLIENT_FOLDER, filename);
        FileOutputStream fos = null;

        try 
        {
            fos = new FileOutputStream(dest);
            
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
        System.out.println("[CLIENT] File saved to: " + dest.getPath());
    }

    /**
     * Lists all regular files currently present in the client folder.
     *
     * The filenames are printed to the console and also returned in a list so
     * that menu logic can validate user choices.
     *
     * @return list of simple filenames located under {@value #CLIENT_FOLDER}
     */
    public List<String> showClientFiles()
    {
        System.out.println("Files on Client");

        File folder = new File(CLIENT_FOLDER);
        List<String> files = new ArrayList<>();

        File[] list = folder.listFiles();

        if(list != null)
            for(File f : list)
                if(f.isFile())
                    files.add(f.getName());

        if(files.isEmpty())
            System.out.println("No files available.");
        else
            for(String name : files)
                System.out.println("- " + name);

        return files;
    }

    /**
     * Requests and returns the list of files currently available on the server.
     *
     * Sends a special "LIST" DATA message, then waits for a DATA response whose
     * payload is a newline-separated list of filenames. On timeout or corrupted
     * responses, the request is retried up to {@value #MAX_RETRIES} times.
     *
     * @return list of filenames advertised by the server (possibly empty)
     * @throws IOException if a non-timeout I/O error occurs while sending or
     *                     receiving the LIST exchange
     */
    private List<String> getServerFileList() throws IOException
    {
        Message listRequest = new Message(Message.DATA, sequenceNum, "LIST".getBytes());
        List<String> files = new ArrayList<>();

        // Retransmit LIST request on timeout or corrupted responses so sabotage
        // mode does not kill the whole client.
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++)
        {
            try
            {
                sendMessage(listRequest);
                Message response = receiveMessage();

                if(response.getMessageType() == Message.DATA)
                {
                    String data = new String(response.getPayload());
                    String[] names = data.split("\n");

                    for(String name : names)
                        if(!name.trim().isEmpty())
                            files.add(name.trim());

                    return files;
                }
                else
                {
                    System.out.println(Colors.yellow("[CLIENT] Unexpected message type while listing server files: " + response.msgTypeString()));
                }
            }
            catch (SocketTimeoutException e)
            {
                System.out.println(Colors.yellow("[CLIENT] Timeout waiting for server file list (attempt "
                        + (attempt + 1) + "/" + MAX_RETRIES + ")"));
            }
            catch (SecurityException | IllegalArgumentException e)
            {
                System.out.println(Colors.red("[CLIENT] Dropped corrupted or malformed LIST response: " + e.getMessage()));
            }
        }

        System.out.println(Colors.red("[CLIENT] Unable to retrieve server file list after " + MAX_RETRIES + " attempts."));
        return files;
    }

    /**
     * Prints and returns the list of files available on the server.
     *
     * Internally calls {@link #getServerFileList()} to perform the LIST request
     * with retry logic, then renders the result in a user-friendly format for
     * the interactive menu.
     *
     * @return list of filenames advertised by the server (possibly empty)
     * @throws IOException if the underlying LIST request fails with an I/O error
     */
    public List<String> showServerFiles() throws IOException
    {
        System.out.println("Files on Server:");

        List<String> serverFiles = getServerFileList();

        if(serverFiles.isEmpty())
        {
            System.out.println("No files available.");
            return serverFiles;
        }

        for(String name : serverFiles)
            System.out.println("- " + name);

        return serverFiles;
    }

    /**
     * Downloads a file from the server and saves it locally to CLIENT_FOLDER.
     *
     * Protocol flow:
     *   1. Client sends DATA(sequenceNum, filename) as a download request.
     *   2. Server responds with ACK(sequenceNum) to acknowledge the request.
     *   3. Client sends ACK(sequenceNum + 1) to confirm it is ready to receive.
     *   4. Server sends DATA packets in stop-and-wait order.
     *   5. Client ACKs each in-order packet; sends duplicate ACK for out-of-order.
     *   6. Server sends FIN when all data is sent; client responds with FIN_ACK.
     *   7. Client reassembles chunks and writes the file.
     *
     * @param remoteFilename  filename to request from the server
     * @param localFilename   local path to save the downloaded file inside CLIENT_FOLDER
     * @return true if the file was downloaded successfully, false otherwise
     * @throws IOException if a network or file-system error occurs
     */
    public boolean downloadFile(String remoteFilename, String localFilename) throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            System.out.println(Colors.red("Cannot start download. Connection is not established."));
            return false;
        }

        System.out.println(Colors.cyan("\n===== Starting File Download ====="));
        System.out.println("Remote: ServerFolder/" + remoteFilename + "  ->  Local: " + CLIENT_FOLDER + "/" + localFilename);

        Message readRequest = new Message(Message.READ, sequenceNum, remoteFilename.getBytes());
        boolean requestAcked = false;

        for(int i = 0; i < MAX_RETRIES && !requestAcked; i++)
        {
            sendMessage(readRequest);
            System.out.println("[CLIENT] Message sent [Type = READ, SeqNum = " + sequenceNum + ", File = '" + remoteFilename + "'" + " (attempt " + (i + 1) + "/" + MAX_RETRIES + ")]");

            try
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.ERROR)
                {
                    String error = new String(response.getPayload());
                    System.out.println(Colors.red("Server error: " + error));
                    return false;
                }
                else if(response.getMessageType() == Message.ACK && response.getSequenceNum() == sequenceNum)
                {
                    System.out.println("Received ACK for download request. SeqNum = " + sequenceNum);
                    requestAcked = true;
                }
                else
                {
                    System.out.println(Colors.yellow("[CLIENT] Unexpected response to download request: "
                            + response.msgTypeString() + " SeqNum = " + response.getSequenceNum()));
                }
            }
            catch(SocketTimeoutException e)
            {
                System.out.println(Colors.yellow("[CLIENT] Timeout waiting for download request ACK. (attempt "
                        + (i + 1) + "/" + MAX_RETRIES + ")"));
            }
            catch(SecurityException | IllegalArgumentException e)
            {
                System.out.println(Colors.red("[CLIENT] Corrupted ACK during download request: " + e.getMessage()));
            }
        }

        if(!requestAcked)
        {
            System.out.println(Colors.red("[CLIENT] Server did not acknowledge download request. Aborting."));
            return false;
        }

        sequenceNum++;
        List<byte[]> receivedChunks = new ArrayList<>();
        int expectedSeq = sequenceNum;       // first data chunk arrives at current sequenceNum
        int lastAckedSeq = sequenceNum - 1;  // nothing data-ACKed yet
        boolean complete = false;
        int timeoutAttempts = 0;

        while(!complete)
        {
            try
            {
                Message response = receiveMessage();

                if(response.getMessageType() == Message.ERROR)
                {
                    String error = new String(response.getPayload());
                    System.out.println(Colors.red("Received server error: " + error));
                    return false;
                }
                else if(response.getMessageType() == Message.DATA)
                {
                    int recvSeqNum = response.getSequenceNum();
                    int recvPayloadLen = response.getPayloadLen();
                    System.out.println("Received DATA packet. SeqNum = " + recvSeqNum
                            + ", Payload Length = " + recvPayloadLen);

                    if(recvSeqNum == expectedSeq)
                    {
                        timeoutAttempts = 0; 
                        receivedChunks.add(response.getPayload());

                        Message ack = new Message(Message.ACK, recvSeqNum);
                        sendMessage(ack);
                        System.out.println("[CLIENT] Message sent [Type = ACK, SeqNum = " + recvSeqNum + "]");

                        lastAckedSeq = recvSeqNum;
                        expectedSeq++;
                    }
                    else
                    {
                        System.out.println(Colors.yellow("Out of order packet. Expected SeqNum = "
                                + expectedSeq + ", Received SeqNum = " + recvSeqNum));

                        Message dupeAck = new Message(Message.ACK, lastAckedSeq);
                        sendMessage(dupeAck);
                        System.out.println("[CLIENT] Sent duplicate ACK for SeqNum = " + lastAckedSeq);
                    }
                }
                else if(response.getMessageType() == Message.FIN)
                {
                    int finSeq = response.getSequenceNum();
                    System.out.println("Received FIN for SeqNum = " + finSeq);

                    Message finAck = new Message(Message.FIN_ACK, finSeq);
                    sendMessage(finAck);
                    System.out.println("[CLIENT] Message sent [Type = FIN_ACK, SeqNum = " + finSeq + "]");

                    complete = true;
                }
                else
                {
                    System.out.println(Colors.yellow("[CLIENT] Unexpected message type during download: "
                            + response.msgTypeString() + " SeqNum = " + response.getSequenceNum()));
                }
            }
            catch(SocketTimeoutException e)
            {
                timeoutAttempts++;
                System.out.println(Colors.yellow("Timeout waiting for DATA. Attempt ("
                        + timeoutAttempts + "/" + MAX_RETRIES + ")"));

                if(timeoutAttempts >= MAX_RETRIES)
                {
                    System.out.println(Colors.red("[CLIENT] Download failed: no data received after "
                            + MAX_RETRIES + " attempts."));
                    break;
                }
            }
            catch(SecurityException | IllegalArgumentException e)
            {
                System.out.println(Colors.red("[CLIENT] Dropped corrupted or malformed packet during download: " + e.getMessage()));
            }
        }

        if(complete)
        {
            writeChunksToFile(receivedChunks, localFilename);
            System.out.println("Total packets received: " + receivedChunks.size());
            System.out.println(Colors.green("FILE DOWNLOAD COMPLETE"));
            return true;
        }

        System.out.println(Colors.red("[CLIENT] FILE DOWNLOAD INCOMPLETE - transfer aborted before FIN."));
        System.out.println(Colors.yellow("[CLIENT] Received " + receivedChunks.size()
                + " data packet(s) before aborting."));
        return false;
    }

    /**
     * Uploads a local file from CLIENT_FOLDER to the server.
     *
     * Protocol flow:
     *  1. Client sends DATA(seq, remoteFilename) as the filename handshake.
     *  2. Server replies with ACK.
     *  3. Client sends file data in stop-and-wait chunks, waiting for ACK after each.
     *  4. After the last chunk is acknowledged, client sends FIN and waits for FIN_ACK.
     *
     * @param localFilename   bare filename inside CLIENT_FOLDER to upload
     * @param remoteFilename  filename to save as on the server
     * @return true if the file was uploaded successfully
     * @throws IOException if a network or file-system error occurs
     */
    public boolean uploadFile(String localFilename, String remoteFilename) throws IOException
    {
        if(state != ClientState.ESTABLISHED)
        {
            System.out.println(Colors.red("Cannot upload file. Connection is not established"));
            return false;
        }

        System.out.println(Colors.cyan("\n===== Starting File Upload ====="));
        System.out.println("Local: " + CLIENT_FOLDER + "/" + localFilename + "  ->   Remote: ServerFolder/" + remoteFilename + "\n");

        File file = new File(CLIENT_FOLDER, localFilename);
        if(!file.exists())
        {
            System.out.println(Colors.red("Error: File not found in " + CLIENT_FOLDER + "/"));
            return false;
        }

        Message filename = new Message(Message.WRITE, sequenceNum, remoteFilename.getBytes());
        boolean filenameAcked = false;

        for (int i = 0; i < MAX_RETRIES && !filenameAcked; i++) 
        {
            sendMessage(filename);
            System.out.println("[CLIENT] Message sent [Type = WRITE, SeqNum = " + sequenceNum + ", File = '" + remoteFilename + "']");
            
            if (waitAck(sequenceNum, i + 1, MAX_RETRIES))
            {
                filenameAcked = true;
            }
        }

        if (!filenameAcked) 
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

            while ((bytesRead = fis.read(buffer)) != -1) 
            {
                byte[] payload = Arrays.copyOf(buffer, bytesRead);  
                Message data = new Message(Message.DATA, sequenceNum, payload);
                boolean acked = false;

                for (int i = 0; i < MAX_RETRIES && !acked; i++) {
                    sendMessage(data);
                    System.out.println("[CLIENT] Message sent [Type = DATA, SeqNum = " + sequenceNum +
                                    ", Payload = " + bytesRead + " bytes]");

                    if (waitAck(sequenceNum, i + 1, MAX_RETRIES))
                    {
                        acked = true;
                    }
                }

                if (!acked) 
                {
                    System.out.println(Colors.red("[CLIENT] Failed to send DATA SeqNum = " + sequenceNum + ". Upload aborted"));
                    return false;
                }
                sequenceNum++;  
            }

            Message fin = new Message(Message.FIN, sequenceNum);
            boolean complete = false;

            for (int i = 0; i < MAX_RETRIES && !complete; i++) 
            {
                sendMessage(fin);
                System.out.println("[CLIENT] Message sent [Type = FIN, SeqNum = " + sequenceNum + "]");

                try {
                    Message response = receiveMessage();
                    if (response.getMessageType() == Message.FIN_ACK) {
                        System.out.println("Received FIN_ACK for SeqNum = " + response.getSequenceNum());
                        complete = true;
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println(Colors.yellow("Timeout waiting for FIN_ACK, retrying (" + (i + 1) + "/" + MAX_RETRIES + ")"));
                } catch (SecurityException | IllegalArgumentException e) {
                    System.out.println(Colors.red("[CLIENT] Corrupted or malformed FIN_ACK during upload: " + e.getMessage()));
                }
            }

            if (complete) {
                System.out.println(Colors.green("FILE UPLOADED SUCCESSFULLY -> ServerFolder/" + remoteFilename));
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

        boolean wasSabotageMode = sabotageMode;
    
        // temporarily disable sabotage for clean termination
        if (sabotageMode) {
            System.out.println(Colors.yellow("[CLIENT] Temporarily disabling sabotage for clean termination."));
            sabotageMode = false;
        }

        Message fin = new Message(Message.FIN, sequenceNum);
        state = ClientState.FIN_WAIT;

        // initial send
        sendMessage(fin);
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
                System.out.println(Colors.yellow("[CLIENT] Timeout waiting for FIN_ACK."
                        + (i < MAX_RETRIES ? " retransmitting FIN..." : "")));
                sendMessage(fin);
                System.out.println("[CLIENT] Message sent [Type = FIN, SeqNum = " + sequenceNum + " (retransmission " + (i + 1) + "/" + MAX_RETRIES + ")]");
            }
            catch (SecurityException | IllegalArgumentException e)
            {
                System.out.println(Colors.red("[CLIENT] Corrupted or malformed FIN_ACK during disconnect: " + e.getMessage()));
            }
        }

        state = ClientState.CLOSED;
        System.out.println(Colors.red("Forced disconnect after timeout."));

        sabotageMode = wasSabotageMode;
    }

     /** Closes the UDP socket and releases system resources. */
    public void close()
    {
        if(UDPsocket != null && !UDPsocket.isClosed())
        {
            UDPsocket.close();
        }
    }

    /**
     * Interactive configuration of sabotage mode parameters.
     * @param scanner Scanner for user input
     */
    private void configureSabotageMode(Scanner scanner)
    {
        System.out.println("\nConfigure sabotage parameters:");
        
        // delay simulation
        System.out.print("Enter artificial delay in ms (0-5000, recommended: 1000): ");
        try
        {
            delayMs = Integer.parseInt(scanner.nextLine().trim());
            
            if(delayMs < 0)
                delayMs = 0;

            if(delayMs > 5000)
                delayMs = 5000;
        }
        catch (NumberFormatException e)
        {
            delayMs = 1000; // default
        }

        // sequence blocking
        System.out.print("Enable sequence blocking? (y/n): ");
        blockSequences = scanner.nextLine().trim().toLowerCase().startsWith("y");

        if(blockSequences)
        {
            System.out.print("Enter SeqNum to block (-1 to disable): ");
            try
            {
                blockedSeqNum = Integer.parseInt(scanner.nextLine().trim());

                if(blockedSeqNum < -1)
                    blockedSeqNum = -1;
            }
            catch(NumberFormatException e)
            {
                blockedSeqNum = -1; // default, no specific seq blocked
            }
        }
        else
        {
            blockedSeqNum = -1;
        }

        System.out.println(Colors.yellow("\n[SABOTAGE] Configuration:")); 
        System.out.println("Artificial delay: " + delayMs + "ms");
        System.out.println("Sequence blocking: " + (blockSequences ? "enabled (blocking SeqNum = " + blockedSeqNum + ")" : "disabled"));
        System.out.println(Colors.yellow("[SABOTAGE] These settings will test timeout, retransmission, and error recovery."));
    }

    /**
     * Presents the user with a choice between Normal Mode (reliable transfer) and Sabotage Mode (introduces delays and packet loss).
     * @param scanner Scanner for user input
     */
    private void selectTestMode(Scanner scanner)
    {
        System.out.println(Colors.cyan("\n===== Test Mode Selection ====="));    
        System.out.println("[1] Normal Mode - Standard reliable transfer");    
        System.out.println("[2] Sabotage Mode - Test reliability and error handling");    

        while (true)
        {
            System.out.print("Select mode (1 or 2): ");
            String choice = scanner.nextLine().trim();

            if("1".equals(choice))
            {
                sabotageMode = false;
                System.out.println(Colors.green("[MODE] Normal mode selected - reliable transfer enabled"));
                break;
            }
            else if("2".equals(choice))
            {
                sabotageMode = true;
                configureSabotageMode(scanner);
                System.out.println(Colors.green("[MODE] Sabotage mode selected - reliability testing enabled"));
                break;
            }
            else
            {
                System.out.println(Colors.yellow("Invalid choice. Please enter 1 for Normal or 2 for Sabotage."));
            }
        }
    }

    /**
     * Application entry point.
     *
     * Prompts the user for connection details, then presents an interactive menu:
     * [1] Download File – lists server files, user picks one to download
     * [2] Upload File   – lists client files, user picks one to upload
     * [X] Disconnect
     *
     * @param args  command-line arguments (not used)
     */
    public static void main (String[] args)
    {
        System.out.println(Colors.cyan("===== Starting CLIENT program ====="));
        Scanner scanner = new Scanner(System.in);
        Client client = null;
        int clientPort = 0; 
        int serverPort = 0;

        while (true) 
        {
            System.out.print("Enter client port: ");
            try 
            {
                clientPort = Integer.parseInt(scanner.nextLine().trim());
                if (clientPort <= 0 || clientPort > 65535) 
                {
                    System.out.println(Colors.red("Invalid port number. Please enter 1–65535."));
                    continue;
                }
                break;
            } 
            catch (NumberFormatException e) 
            {
                System.out.println(Colors.red("Invalid input. Please enter a valid number for port."));
            }
        }

        try 
        {
            client = new Client(clientPort);
        } 
        catch (Exception e) 
        {
            System.out.println(Colors.red("Failed to initialize client socket: " + e.getMessage()));
            return;
        }

        System.out.print("Enter server host (localhost): ");
        String serverHost = scanner.nextLine().trim();

        if (serverHost.isEmpty()) 
        {
            serverHost = "localhost";
        }

        while (true) 
        {
            System.out.print("Enter server port: ");
            try 
            {
                serverPort = Integer.parseInt(scanner.nextLine().trim());
                if (serverPort <= 0 || serverPort > 65535) 
                {
                    System.out.println(Colors.red("Invalid port number. Please enter 1–65535."));
                    continue;
                }
                break;
            } 
            catch (NumberFormatException e) 
            {
                System.out.println(Colors.red("Invalid input. Please enter a valid number for port."));
            }
        }
            
        try 
        {
            if (!client.connect(serverHost, serverPort)) 
            {
                System.out.println(Colors.red("Failed to establish connection to server."));
                return;
            }

            boolean flag = false;
            while (!flag) 
            {
                System.out.println(Colors.cyan("\n===== File Transfer Functionality ====="));
                System.out.println("[1] Download File");
                System.out.println("[2] Upload File");
                System.out.println("[X] Disconnect");
                System.out.print("Choose option: ");

                String choice = scanner.nextLine().trim();

                switch (choice) 
                {
                    case "1":
                        client.selectTestMode(scanner);

                        System.out.println(Colors.cyan("\n===== Download File ====="));

                        List<String> files = client.showServerFiles();
                        if(files.isEmpty())
                            break;

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
                        client.selectTestMode(scanner);

                        System.out.println(Colors.cyan("\n===== Upload File ====="));
                            
                        List<String> localFiles = client.showClientFiles();
                        if(localFiles.isEmpty())
                            break;

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

        } 
        catch (UnknownHostException e)
        {
            System.out.println(Colors.red("Invalid server hostname: " + e.getMessage()));
            System.out.println(Colors.yellow("Please restart the client and enter a valid host (e.g., localhost or 127.0.0.1)."));
    
        } 
        catch (Exception e) 
        {
            System.out.println(Colors.red("Error: " + e.getMessage()));
        } 
        finally 
        {
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

    /** dedicated directory where all client-side files are stored/saved */
    private static final String CLIENT_FOLDER = "ClientFolder";

    private boolean sabotageMode = false;
    private int delayMs = 0;
    private boolean blockSequences = false;
    private int blockedSeqNum = -1;
}