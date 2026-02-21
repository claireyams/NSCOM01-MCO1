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
    private enum ServerState
    {
        LISTEN, ESTABLISHED, CLOSE_WAIT
    }

    private DatagramSocket UDPsocket;
    private InetAddress clientAddress;
    private int clientPort;
    private ServerState state;

    private static final int TIMEOUT = 5000; // 5 seconds
    private static final int MAX_RETRIES = 3;
    private static final int BUFFER_SIZE = 1024;
    private static final int MAX_PAYLOAD_SIZE = 1000;

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

        return Message.convertToMessage(Arrays.copyOf(packet.getData(), packet.getLength()));
    }

    public void start() throws IOException
    {
        System.out.println("===== Server Started =====");

        while (true)
        {
            Message msg = receiveMessage();

            if (state == ServerState.LISTEN && msg.getMessageType() == Message.SYN)
            {
                System.out.println("[SERVER] Received SYN, SeqNum = " + msg.getSequenceNum());

                Message synAck = new Message(Message.SYN_ACK, msg.getSequenceNum());
                sendMessage(synAck);

                System.out.println("[SERVER] Sent SYN_ACK");
                state = ServerState.ESTABLISHED;
            }

            else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.DATA)
            {
                handleDownloadRequest(msg);
            }

            else if (state == ServerState.ESTABLISHED && msg.getMessageType() == Message.FIN)
            {
                System.out.println("[SERVER] Received FIN");

                Message finAck = new Message(Message.FIN_ACK, msg.getSequenceNum());
                sendMessage(finAck);

                System.out.println("[SERVER] Sent FIN_ACK");
                state = ServerState.LISTEN;
            }
        }
    }

    private void handleDownloadRequest(Message request) throws IOException
    {
        String filename = new String(request.getPayload()).trim();
        File file = new File(filename);

        if (!file.exists())
        {
            Message error = new Message(Message.ERROR, request.getSequenceNum(),
                    ("File not found: " + filename).getBytes());
            sendMessage(error);

            System.out.println("[SERVER] File not found: " + filename);
            return;
        }

        System.out.println("[SERVER] Sending file: " + filename);

        FileInputStream fis = new FileInputStream(file);
        byte[] buffer = new byte[MAX_PAYLOAD_SIZE];
        int bytesRead;
        int seq = 0;

        while ((bytesRead = fis.read(buffer)) != -1)
        {
            byte[] payload = Arrays.copyOf(buffer, bytesRead);
            Message data = new Message(Message.DATA, seq, payload);

            boolean acked = false;

            for (int i = 0; i < MAX_RETRIES && !acked; i++)
            {
                sendMessage(data);
                System.out.println("[SERVER] Sent DATA Seq=" + seq);

                try
                {
                    Message ack = receiveMessage();

                    if (ack.getMessageType() == Message.ACK && ack.getSequenceNum() == seq)
                    {
                        System.out.println("[SERVER] Received ACK for Seq=" + seq);
                        acked = true;
                    }
                }
                catch (SocketTimeoutException e)
                {
                    System.out.println("[SERVER] Timeout waiting for ACK, retrying...");
                }
            }

            if (!acked)
            {
                System.out.println("[SERVER] Transfer failed (no ACK).");
                fis.close();
                return;
            }

            seq++;
        }

        fis.close();

        Message fin = new Message(Message.FIN, seq);
        sendMessage(fin);
        System.out.println("[SERVER] Sent FIN (File transfer complete)");
    }

    public void close()
    {
        if (UDPsocket != null && !UDPsocket.isClosed())
            UDPsocket.close();
    }

    public static void main(String[] args)
    {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter server port: ");
        int port = scanner.nextInt();

        Server server = null;

        try
        {
            server = new Server(port);
            server.start();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (server != null)
                server.close();
            scanner.close();
        }
    }
}