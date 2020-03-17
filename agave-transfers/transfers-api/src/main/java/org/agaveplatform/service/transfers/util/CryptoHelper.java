package org.agaveplatform.service.transfers.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CryptoHelper {

  public static String publicKey() throws IOException {
    return read("public_key.pem");
  }

  public static String privateKey() throws IOException {
    return read("private_key.pem");
  }

  private static String read(String file) throws IOException {
    Path keyPath = Paths.get(CryptoHelper.class.getClassLoader().getResource(file).getPath());
    return Files
              .readAllLines(keyPath)
              .stream()
              .filter(line -> !line.startsWith("-----"))
              .reduce(String::concat)
              .orElse("OUPS");
  }
}
