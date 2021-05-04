package org.agaveplatform.service.transfers.nats;

import io.nats.client.AuthHandler;
import io.nats.client.NKey;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

public class NatsAuthHandler implements AuthHandler {
    private final NKey nkey;

    public NatsAuthHandler(String path) throws Exception {
        File f = new File(path);
        int numBytes = (int)f.length();
        try (BufferedReader in = new BufferedReader(new FileReader(f))) {
            char[] buffer = new char[numBytes];
            int numChars = in.read(buffer);
            if (numChars < numBytes) {
                char[] seed = new char[numChars];
                System.arraycopy(buffer, 0, seed, 0, numChars);
                this.nkey = NKey.fromSeed(seed);
                Arrays.fill(seed, '\0'); // clear memory
            }
            else {
                this.nkey = NKey.fromSeed(buffer);
            }
            Arrays.fill(buffer, '\0'); // clear memory
        }
    }

    public NKey getNKey() {
        return this.nkey;
    }

    public char[] getID() {
        try {
            return this.nkey.getPublicKey();
        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
            return null;
        }
    }

    public byte[] sign(byte[] nonce) {
        try {
            return this.nkey.sign(nonce);
        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
            return null;
        }
    }

    public char[] getJWT() {
        return null;
    }
}
