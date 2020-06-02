package org.agaveplatform.service.transfers.util;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;

public class CryptoHelper {

  /**
   * A random RSA keypair generated for this instance
   */
  private KeyPair keyPair;

  /**
   * No-arg constructor
   */
  public CryptoHelper() {}

  /**
   * Generates a new RSA {@link KeyPair} instance to use for JWT creation and validation.
   * @return a generated keypair for this instance
   * @throws NoSuchAlgorithmException if no RSA algorithm is implemented in the classpath
   */
  private KeyPair getKeyPair() throws NoSuchAlgorithmException {
    if (keyPair == null) {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(2048);
      keyPair = keyPairGenerator.generateKeyPair();
    }
    return keyPair;
  }

  /**
   * Returns serialized PEM value of {@link RSAPublicKey} for the generated {@link KeyPair}.
   * @return the public key from the generated keypair
   */
  public String getPublicKey() {
    String pubKey = null;
    try {
      pubKey = writeKey(getKeyPair().getPublic(), "PUBLIC KEY");
//      System.out.println("Public key: " + pubKey);
    } catch (NoSuchAlgorithmException|IOException ignored) {}
    return pubKey;
  }

  /**
   * Returns serialized PEM value of {@link RSAPublicKey} for the generated {@link KeyPair}.
   * @return the private key from the generated keypair
   */
  public String getPrivateKey() {
    String pemKey = null;
    try {
      pemKey = writeKey(getKeyPair().getPrivate(), "PRIVATE KEY");
//      System.out.println("Private key: " + pemKey);
    } catch (NoSuchAlgorithmException|IOException ignored) {}
      return pemKey;
  }

  /**
   * Serializes the key to a PEM string
   * @param key the key to serialize
   * @param description the pem file header
   * @return the serialized pem value of the key
   * @throws IOException if the value cannot be serialized
   */
  private String writeKey(Key key, String description) throws IOException {
    String pemKey = "";

    PemObject pemObject = new PemObject(description, key.getEncoded());
    try (StringWriter sw = new StringWriter();) {

      PemWriter pemWriter = new PemWriter(sw);
      pemWriter.writeObject(pemObject);
      pemWriter.close();
      pemKey = sw.toString();

    }
    return pemKey;
  }

  /**
   * Returns public key from the classpath with name {@code public_key.pem} stripped of header and footer lines.
   * @return file contents stripped of pem header and footer
   * @throws IOException if file is not present or cannot be read.
   */
  public static String publicKey() throws IOException {
    String bundledResourcePath = "public_key.pem";
    Path keyPath = Paths.get(CryptoHelper.class.getClassLoader().getResource(bundledResourcePath).getPath());
    return read(keyPath);
  }

  /**
   * Returns public key from the classpath with name {@code public_key.pem} stripped of header and footer lines.
   * @param publicKeyFile the path to the pem-encoded public key file on disk
   * @return file contents stripped of pem header and footer
   * @throws IOException if file is not present or cannot be read.
   */
  public static String publicKey(String publicKeyFile) throws IOException {
    Path keyPath = Paths.get(publicKeyFile);
    return read(keyPath);
  }

  /**
   * Returns private key from the classpath with name {@code private_key.pem} stripped of header and footer lines.
   * @return file contents stripped of pem header and footer
   * @throws IOException if file is not present or cannot be read.
   */
  public static String privateKey() throws IOException {
    String bundledResourcePath = "private_key.pem";
    Path keyPath = Paths.get(CryptoHelper.class.getClassLoader().getResource(bundledResourcePath).getPath());
    return read(keyPath);
  }

  /**
   * Returns private key from the classpath with name {@code private_key.pem} stripped of header and footer lines.
   * @param privateKeyFile the path to the pem-encoded private key file on disk
   * @return file contents stripped of pem header and footer
   * @throws IOException if file is not present or cannot be read.
   */
  public static String privateKey(String privateKeyFile) throws IOException {
    Path keyPath = Paths.get(privateKeyFile);
    return read(keyPath);
  }

  /**
   * Reads contents of file at the given {@code keyPath}, returning the contents stripped of the header and footer
   * @param keyPath the path to the PEM file to read on disk.
   * @return the contents of the file stripped of the PEM header and footer.
   * @throws IOException if the file cannot be read
   */
  private static String read(Path keyPath) throws IOException {

    return Files
              .readAllLines(keyPath)
              .stream()
              .filter(line -> !line.startsWith("-----"))
              .reduce(String::concat)
              .orElse("PEM file contents should have been here");
  }
}
