import javax.crypto.*;
import javax.crypto.spec.*;
import java.security.*;
import java.util.Arrays;

/**
 *  The class Cryptography provides AES-128 encryption and decryption for all
 *  protocol messages transmitted over UDP.
 *
 *  All messages (control and data alike) are encrypted before being placed into
 *  a DatagramPacket and decrypted immediately after being received. This means
 *  any observer capturing UDP packets on the network sees only ciphertext.
 *
 *  Algorithm: AES-128 in CBC mode with PKCS5 padding.
 *
 *  Key management: both client and server use the same SHARED_KEY constant,
 *  simulating a Pre-Shared Key (PSK) authentication model.
 *
 *  IV handling: a fresh 16-byte random IV is generated for every encrypt() call
 *  and prepended to the ciphertext. decrypt() reads the first 16 bytes as the IV
 *  before decrypting the remainder. This prevents identical plaintexts from
 *  producing identical ciphertexts.
 *
 *  @author Sky Hannah Parado
 *  @author Rhian Claire Yamsuan
 *  @version 1.0
 */
public class Cryptography
{
    // Set to false when you want Wireshark to show raw packet bytes/header fields.
    // Set back to true for encrypted transport.
    private static final boolean ENCRYPTION_ENABLED = false;

    private static final byte[] SHARED_KEY = "NSCOM01sharedkey".getBytes(); // 16 bytes since AES requires exactly 16, 24, or 32 bytes
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding"; // AES algorithm with CBC mode and PKCS5 padding
    private static final int IV_SIZE = 16;

    /**
     * Encrypts a byte array using AES-128/CBC/PKCS5Padding.
     *
     * A fresh random IV is generated for each call and prepended to the output
     * so that decrypt() can extract it without a separate channel.
     * Output format: [16 bytes IV] + [N bytes ciphertext]
     *
     * @param data  plaintext bytes to encrypt (a serialized Message)
     * @return      IV-prepended ciphertext ready to place in a DatagramPacket
     */
    public static byte[] encrypt(byte[] data)
    {
        if (!ENCRYPTION_ENABLED) return data;

        try
        {
            byte[] iv = new byte[IV_SIZE];
            new SecureRandom().nextBytes(iv);
            IvParameterSpec ivSpec = new IvParameterSpec(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(SHARED_KEY, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            byte[] ciphertext = cipher.doFinal(data);

            // prepend the IV 
            byte[] result = new byte[IV_SIZE + ciphertext.length];
            System.arraycopy(iv, 0, result, 0, IV_SIZE);
            System.arraycopy(ciphertext, 0, result, IV_SIZE, ciphertext.length);
            
            return result;
        }
        catch(Exception e)
        {
            throw new RuntimeException("Encryption failed: " + e.getMessage(), e);
        }
    }

    /**
     * Decrypts an IV-prepended AES-128/CBC ciphertext back to plaintext.
     *
     * Expects input in the format produced by encrypt():
     * [16 bytes IV] + [N bytes ciphertext]
     *
     * @param data  IV-prepended ciphertext from a DatagramPacket
     * @return      original plaintext bytes (a serialized Message)
     */
    public static byte[] decrypt(byte[] data)
    {
        if (!ENCRYPTION_ENABLED) return data;

        try 
        {
            if (data.length < IV_SIZE)
            {
                throw new SecurityException("Packet too short for encrypted format.");
            }

            // extract the IV from the first 16 bytes 
            byte[] iv = Arrays.copyOfRange(data, 0, IV_SIZE);
            byte[] ciphertext = Arrays.copyOfRange(data, IV_SIZE, data.length);

            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(SHARED_KEY, "AES");
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);

            return cipher.doFinal(ciphertext);
        }
        catch (SecurityException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new SecurityException("Decryption failed: " + e.getMessage());
        }
    }
}
