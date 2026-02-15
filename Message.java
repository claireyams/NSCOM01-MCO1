import java.io.*;
import java.nio.ByteBuffer;

/**
 *  The class Message represents a message in the reliable data transfer protocol over UDP.
 * 
 *  @author Sky Hannah Parado
 *  @author Rhian Claire Yamsuan
 *  @version 1.0
 */

public class Message implements Serializable
{
    // message constructor
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

    // another message constructor
    public Message(byte mtype, int snum)
    {
        this(mtype, snum, null);
    }

    // converts a message object into a byte array that can be sent over the network
    public byte[] convertToBytes()
    {
        // allocate 9 bytes (1 for messageType, 4 for sequenceNum, and 4 for payloadLen) for the header + payload length)
        ByteBuffer buffer = ByteBuffer.allocate(9 + payloadLen);
        buffer.put(messageType);
        buffer.putInt(sequenceNum);
        buffer.putInt(payloadLen);

        if(payload != null)
        {
            buffer.put(payload);
        }

        return buffer.array();
    }

    // converts received bytes back into a Message object
    public static Message convertToMessage(byte[] data)
    {
        if(data.length < 9)
        {
            throw new IllegalArgumentException("Message is too short.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte messageType = buffer.get();
        int sequenceNum = buffer.getInt();
        int payloadLen = buffer.getInt();

        byte[] payload = null;

        if(payloadLen > 0 && buffer.remaining() >= payloadLen)
        {
            payload = new byte[payloadLen];
            buffer.get(payload);
        }

        return new Message(messageType, sequenceNum, payload);
    }

    // return the message type in string
    public String msgTypeString()
    {
        switch(messageType)
        {
            case SYN: return "SYN";
            case SYN_ACK: return "SYN_ACK";
            case DATA: return "DATA";
            case ACK: return "ACK";
            case FIN: return "FIN";
            case FIN_ACK: return "FIN_ACK";
            case ERROR: return "ERROR";
            default: return "UNKNOWN";
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

    public byte[] payload()
    {
        return payload;
    }

    // message types
    public static final byte SYN = 0x01;
    public static final byte SYN_ACK = 0x02;
    public static final byte DATA = 0x03;
    public static final byte ACK = 0x04;
    public static final byte FIN = 0x05;
    public static final byte FIN_ACK = 0x06;
    public static final byte ERROR = 0x07;

    // datagram header fields
    private byte messageType;
    private int sequenceNum;
    private int payloadLen;
    private byte[] payload;
}