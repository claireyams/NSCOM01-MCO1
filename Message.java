import java.io.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.Arrays;

/**
 *  The class Message represents a message in the reliable data transfer protocol over UDP.
 * 
 *  Each message has a fixed 9-byte header followed by the payload and a 32-byte
 *  SHA-256 hash of the payload, appended at the end.
 *
 *  Wire format:
 *    [1 byte  messageType ]
 *    [4 bytes sequenceNum ]
 *    [4 bytes payloadLen  ]
 *    [N bytes payload     ]  (N = payloadLen)
 *    [32 bytes SHA-256    ]  (hash of payload only)
 *
 *  On receipt, convertToMessage() recomputes the SHA-256 of the payload and
 *  compares it against the appended hash. A mismatch means the packet was
 *  corrupted or tampered with in transit, and a SecurityException is thrown.
 *  
 *  @author Sky Hannah Parado
 *  @author Rhian Claire Yamsuan
 *  @version 1.0
 */

public class Message implements Serializable
{
    /**
     * Full constructor for messages that carry a payload.
     *
     * @param mtype  one of the message-type constants defined in this class
     * @param snum   sequence number for this message
     * @param pload  payload bytes, or null for control messages
     */
    public Message(byte mtype, int snum, byte[] pload)
    {
        messageType = mtype;
        sequenceNum = snum;
        payload = pload;
        
        if(pload != null)
        {
            payloadLen = pload.length;
        }
        else
        {
            payloadLen = 0;
        }
    }

    /**
     * Convenience constructor for control messages that carry no payload
     * (SYN, SYN_ACK, ACK, FIN, FIN_ACK).
     *
     * @param mtype  one of the message-type constants defined in this class
     * @param snum   sequence number for this message
     */
    public Message(byte mtype, int snum)
    {
        this(mtype, snum, null);
    }

    /**
     * Computes a SHA-256 hash of the given data for per-packet integrity checking.
     * If data is null the hash of an empty byte array is returned.
     *
     * @param data  bytes to hash (may be null)
     * @return      32-byte SHA-256 digest
     */
    private static byte[] computeHash(byte[] data)
    {
        try
        {
            MessageDigest msgDigest = MessageDigest.getInstance("SHA-256");

            if(data != null)
            {
                return msgDigest.digest(data);
            }

            return msgDigest.digest(new byte[0]);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts this Message into a byte array suitable for UDP transmission.
     *
     * Appends a 32-byte SHA-256 hash of the payload at the end so the receiver
     * can verify the packet was not corrupted or altered in transit.
     *
     * @return serialized message bytes: 9-byte header + payload + 32-byte hash
     */
    public byte[] convertToBytes()
    {
        try 
        {
            byte[] hash = computeHash(payload);

            // 9 bytes header + payload bytes + 32-byte SHA-256 hash
            ByteBuffer buffer = ByteBuffer.allocate(9 + payloadLen + hash.length);
            buffer.put(messageType);
            buffer.putInt(sequenceNum);
            buffer.putInt(payloadLen);

            if(payload != null)
            {
                buffer.put(payload);
            }

            buffer.put(hash);
            return buffer.array();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reconstructs a Message from raw bytes received over UDP.
     *
     * After parsing the header and payload, re-computes the SHA-256 hash of the
     * payload and compares it to the 32-byte hash appended to the packet.
     * 
     * @param data  raw bytes from a DatagramPacket (after AES decryption)
     * @return      parsed and integrity-verified Message
     * @throws IllegalArgumentException if the byte array is too short
     * @throws SecurityException        if SHA-256 verification fails
     */
    public static Message convertToMessage(byte[] data)
    {
        if(data.length < 9 + 32)
        {
            throw new IllegalArgumentException("Message is too short to contain a valid header and hash.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte messageType = buffer.get();
        int sequenceNum = buffer.getInt();
        int payloadLen = buffer.getInt();

        byte[] payload = null;

        if(payloadLen > 0)
        {
            payload = new byte[payloadLen];
            buffer.get(payload);
        }

        byte[] receivedHash = new byte[32];
        buffer.get(receivedHash);

        // recompute the hash of the payload and compare against the appended hash.
        if(!Arrays.equals(receivedHash, computeHash(payload)))
        {
            System.out.println(Colors.red("[INTEGRITY] SHA-256 MISMATCH. Packet corrupted or Tampered | SeqNum = " + sequenceNum));
            throw new SecurityException("Hash verification failed for SeqNum = " + sequenceNum);
        }

        System.out.println(Colors.green("[INTEGRITY] SHA-256 VERIFIED | SeqNum = " + sequenceNum));
        return new Message(messageType, sequenceNum, payload);
    }

    /**
     * Returns the message type as a human-readable string (e.g. "SYN_ACK").
     *
     * @return message type name
     */
    public String msgTypeString()
    {
        switch(messageType)
        {
            case SYN:     return "SYN";
            case SYN_ACK: return "SYN_ACK";
            case DATA:    return "DATA";
            case ACK:     return "ACK";
            case FIN:     return "FIN";
            case FIN_ACK: return "FIN_ACK";
            case ERROR:   return "ERROR";
            case READ:    return "READ";
            case WRITE:   return "WRITE";
            default:      return "UNKNOWN";
        }
    }

    @Override
    public String toString()
    {
        return String.format("Message[Type = %s, Sequence Number = %d, Payload Length = %d]", msgTypeString(), sequenceNum, payloadLen);
    }

    // getters
    public byte getMessageType()
    {
        return messageType;
    }

    public int getSequenceNum()
    {
        return sequenceNum;
    }

    public int getPayloadLen()
    {
        return payloadLen;
    }

    public byte[] getPayload()
    {
        return payload;
    }

    // message types
    public static final byte SYN     = 0x01;
    public static final byte SYN_ACK = 0x02;
    public static final byte DATA    = 0x03;
    public static final byte ACK     = 0x04;
    public static final byte FIN     = 0x05;
    public static final byte FIN_ACK = 0x06;
    public static final byte ERROR   = 0x07;
    public static final byte READ    = 0x08;
    public static final byte WRITE   = 0x09;

    // datagram header fields
    private byte messageType;
    private int sequenceNum;
    private int payloadLen;
    private byte[] payload;
}