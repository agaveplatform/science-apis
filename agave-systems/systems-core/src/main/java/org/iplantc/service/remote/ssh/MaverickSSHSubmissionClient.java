package org.iplantc.service.remote.ssh;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sshtools.net.ForwardingClient;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.remote.ssh.net.SocketWrapper;
import org.iplantc.service.remote.ssh.shell.Shell;
import org.iplantc.service.remote.ssh.shell.ShellProcess;
import org.iplantc.service.systems.Settings;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.sftp.MaverickSFTPLogger;
import org.iplantc.service.transfer.sftp.MultiFactorKBIRequestHandler;

import com.sshtools.logging.LoggerFactory;
import com.sshtools.publickey.SshPrivateKeyFile;
import com.sshtools.publickey.SshPrivateKeyFileFactory;
import com.sshtools.ssh.PasswordAuthentication;
import com.sshtools.ssh.PublicKeyAuthentication;
import com.sshtools.ssh.SshAuthentication;
import com.sshtools.ssh.SshClient;
import com.sshtools.ssh.SshConnector;
import com.sshtools.ssh.SshSession;
import com.sshtools.ssh.SshTransport;
import com.sshtools.ssh.SshTunnel;
import com.sshtools.ssh.components.ComponentManager;
import com.sshtools.ssh.components.SshKeyPair;
import com.sshtools.ssh.components.jce.JCEComponentManager;
import com.sshtools.ssh2.KBIAuthentication;
import com.sshtools.ssh2.Ssh2Client;
import com.sshtools.ssh2.Ssh2Context;
import com.sshtools.ssh2.Ssh2PublicKeyAuthentication;


/**
 * Client to execute commands on remote systems via SSH. Tunneling is supported
 * when a proxyHost and proxyPort are specified. The authentication to the proxy
 * server is assumed to be the same as the target hostname.
 *
 * @author Rion Dooley <dooley@tacc.utexas.edu>
 */
public class MaverickSSHSubmissionClient implements RemoteSubmissionClient {
    private static final Logger log = Logger.getLogger(MaverickSSHSubmissionClient.class);

    // Set the logging level for the maverick library code.
    // Comment out this static initializer to turn off 
    // maverick library logging.
    static {
        initializeMaverickSFTPLogger();
    }

    // These constants are used to dynamically adjust socket timeouts based on attempt number.
    private static final int TIMEOUT_MULTIPLIER = 5;    // Multiplicative incrementor
    private static final int TIMEOUT_MAX_SECS = 6000; // 100 minutes

    // This map uses command ids as keys and the number of previous timed out attempts
    // as values.  The command id is a string that does not guarantee uniqueness, but
    // is very likely to be so at any time in practice.
    private static final ConcurrentHashMap<String, AtomicInteger> _attemptMap = new ConcurrentHashMap<>();

    private Ssh2Client ssh2 = null;
    private SshSession session;
    private SshConnector con;
    private SshClient forwardedConnection = null;
    private SshAuthentication auth;
    private Socket transport = null;

    protected String hostname;
    protected int port;
    protected String username;
    protected String password;
    protected String proxyHost;
    protected int proxyPort;
    protected String publicKey;
    protected String privateKey;

    public MaverickSSHSubmissionClient(String host, int port, String username,
                                       String password) {
        this.hostname = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public MaverickSSHSubmissionClient(String host, int port, String username,
                                       String password, String proxyHost, int proxyPort) {
        this.hostname = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
    }

    public MaverickSSHSubmissionClient(String host, int port, String username,
                                       String password, String proxyHost, int proxyPort, String publicKey, String privateKey) {
        this.hostname = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
    }

    private boolean authenticate(int soTimeoutMillis) throws RemoteExecutionException {
        try {
            // Get a connector.
            try {
                con = SshConnector.createInstance();
            } catch (Exception e) {
                String msg = getMsgPrefix() + "Unable to create SshConnector instance: " + e.getMessage();
                log.error(msg, e);
                throw e;
            }

            // Get a component manager.
            JCEComponentManager cm;
            try {
                cm = (JCEComponentManager) ComponentManager.getInstance();
            } catch (Exception e) {
                String msg = getMsgPrefix() + "Unable to create ComponentManager instance: " + e.getMessage();
                log.error(msg, e);
                throw e;
            }

            cm.installArcFourCiphers(cm.supportedSsh2CiphersCS());
            cm.installArcFourCiphers(cm.supportedSsh2CiphersSC());

            try {
                con.getContext().setPreferredKeyExchange(Ssh2Context.KEX_DIFFIE_HELLMAN_GROUP14_SHA1);

                con.getContext().setPreferredPublicKey(Ssh2Context.PUBLIC_KEY_SSHDSS);
                con.getContext().setPublicKeyPreferredPosition(Ssh2Context.PUBLIC_KEY_ECDSA_521, 1);

                con.getContext().setPreferredCipherCS(Ssh2Context.CIPHER_ARCFOUR_256);
                con.getContext().setCipherPreferredPositionCS(Ssh2Context.CIPHER_ARCFOUR, 1);
                con.getContext().setCipherPreferredPositionCS(Ssh2Context.CIPHER_AES128_CTR, 1);

                con.getContext().setPreferredCipherSC(Ssh2Context.CIPHER_ARCFOUR_256);
                con.getContext().setCipherPreferredPositionSC(Ssh2Context.CIPHER_ARCFOUR, 1);
                con.getContext().setCipherPreferredPositionCS(Ssh2Context.CIPHER_AES128_CTR, 1);

                con.getContext().setPreferredMacCS(Ssh2Context.HMAC_SHA256);
                con.getContext().setMacPreferredPositionCS(Ssh2Context.HMAC_SHA1, 1);
                con.getContext().setMacPreferredPositionCS(Ssh2Context.HMAC_MD5, 2);

                con.getContext().setPreferredMacSC(Ssh2Context.HMAC_SHA256);
                con.getContext().setMacPreferredPositionSC(Ssh2Context.HMAC_SHA1, 1);
                con.getContext().setMacPreferredPositionSC(Ssh2Context.HMAC_MD5, 2);
            } catch (Exception e) {
                String msg = getMsgPrefix() + "Failure setting a cipher preference: " + e.getMessage();
                log.error(msg, e);
                throw e;
            }

            // Initialize socket.
            SocketAddress sockaddr = null;
            transport = new Socket();
            if (useTunnel()) {
                sockaddr = new InetSocketAddress(proxyHost, proxyPort);
            } else {
                sockaddr = new InetSocketAddress(hostname, port);
            }

            // Timeouts may get adjusted on retries.
            if (log.isDebugEnabled())
                log.debug("Setting socket timeout to " + soTimeoutMillis + "ms.");

            // Configure the socket.
            //  - No delay means send each buffer without waiting to fill a packet.
            try {
                transport.setTcpNoDelay(true);
                transport.setSoTimeout(soTimeoutMillis);
                transport.connect(sockaddr, 15000);
            } catch (Exception e) {
                String msg = getMsgPrefix() + "Socket connection failure: " + e.getMessage();
                log.error(msg, e);
                throw e; // sock is closed in final catch clause.
            }

            // Use the connected socket to perform the ssh handshake.
            try {
                ssh2 = (Ssh2Client) con.connect(((SshTransport) new SocketWrapper(transport)), username, true);
            } catch (Exception e) {
                String msg = getMsgPrefix() + "Failure during ssh initialization: " + e.getMessage();
                log.error(msg, e);
                throw e;
            }

            String[] authenticationMethods;
            try {
                authenticationMethods = ssh2.getAuthenticationMethods(username);
            } catch (Exception e) {
                String msg = getMsgPrefix() + "Failure to get ssh2 authentication methods: " + e.getMessage();
                log.error(msg, e);
                throw e;
            }

            int authStatus;
            if (!StringUtils.isEmpty(publicKey) && !StringUtils.isEmpty(privateKey)) {
                /*
                 * Authenticate the user using password authentication
                 */
                auth = new Ssh2PublicKeyAuthentication();

                do {
                    SshPrivateKeyFile pkfile;
                    try {
                        pkfile = SshPrivateKeyFileFactory.parse(privateKey.getBytes());
                    } catch (Exception e) {
                        String msg = getMsgPrefix() + "Failure to parse private key: " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    // Create the key pair.
                    SshKeyPair pair;
                    try {
                        if (pkfile.isPassphraseProtected()) {
                            pair = pkfile.toKeyPair(password);
                        } else {
                            pair = pkfile.toKeyPair(null);
                        }
                    } catch (Exception e) {
                        String msg = getMsgPrefix() + "Failure to create key pair: " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    ((PublicKeyAuthentication) auth).setPrivateKey(pair.getPrivateKey());
                    ((PublicKeyAuthentication) auth).setPublicKey(pair.getPublicKey());

                    // Authenticate.
                    try {
                        authStatus = ssh2.authenticate(auth);
                    } catch (Exception e) {
                        String msg = getMsgPrefix() + "Failure to authenticate using key pair: " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    if (authStatus == SshAuthentication.FURTHER_AUTHENTICATION_REQUIRED &&
                            Arrays.asList(authenticationMethods).contains("keyboard-interactive")) {
                        KBIAuthentication kbi = new KBIAuthentication();
                        kbi.setUsername(username);
                        kbi.setKBIRequestHandler(new MultiFactorKBIRequestHandler(password, null, username, hostname, port));
                        try {
                            authStatus = ssh2.authenticate(kbi);
                        } catch (Exception e) {
                            String msg = getMsgPrefix() + "Failure to MFA authenticate using key pair: " + e.getMessage();
                            log.error(msg, e);
                            throw e;
                        }
                    }
                } while (authStatus != SshAuthentication.COMPLETE &&
                        authStatus != SshAuthentication.FAILED &&
                        authStatus != SshAuthentication.CANCELLED &&
                        ssh2.isConnected());
            } else {
                /*
                 * Authenticate the user using password authentication
                 */
                auth = new com.sshtools.ssh.PasswordAuthentication();
                do {
                    ((PasswordAuthentication) auth).setPassword(password);

                    auth = checkForPasswordOverKBI(authenticationMethods);

                    try {
                        authStatus = ssh2.authenticate(auth);
                    } catch (Exception e) {
                        String mfa = (auth instanceof PasswordAuthentication) ? "" : "MFA ";
                        String msg = getMsgPrefix() + "Failure to " + mfa + "authenticate using a password: " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }
                } while (authStatus != SshAuthentication.COMPLETE &&
                        authStatus != SshAuthentication.FAILED &&
                        authStatus != SshAuthentication.CANCELLED &&
                        ssh2.isConnected());
            }

            // Record failed authentication attempts in non-tunneled cases.
//			if (!useTunnel()) {
            boolean result = ssh2.isAuthenticated();
            if (!result) {
                String msg = getMsgPrefix() + "Failed to authenticate.";
                log.error(msg);
            }
            return result;
//			}

//			// ------ Now handle the tunnel ------
//            SshTunnel tunnel;
//            try {tunnel = ssh2.openForwardingChannel(hostname, port, "127.0.0.1", port, "127.0.0.1", port, null, null);}
//                catch (Exception e) {
//                    String msg = getMsgPrefix() + "Failure open forwarding channel using proxy: " + e.getMessage();
//                    log.error(msg, e);
//                    throw e;
//                }
//
//            // Connect using the tunnel.
//            try {forwardedConnection = con.connect(tunnel, username);}
//                catch (Exception e) {
//                    String msg = getMsgPrefix() + "Failure to connect using proxy: " + e.getMessage();
//                    log.error(msg, e);
//                    throw e;
//                }

//			if (StringUtils.isNotBlank(publicKey) && StringUtils.isNotBlank(privateKey))
//			{
//				/**
//				 * Authenticate the user using public key authentication
//				 */
//				auth = new Ssh2PublicKeyAuthentication();
//
//				do {
//	                try {authStatus = forwardedConnection.authenticate(auth);}
//                    catch (Exception e) {
//                        String msg = getMsgPrefix() + "Failure to authenticate using proxy: " + e.getMessage();
//                        log.error(msg, e);
//                        throw e;
//                    }
//
//					if (authStatus == SshAuthentication.FURTHER_AUTHENTICATION_REQUIRED &&
//							Arrays.asList(authenticationMethods).contains("keyboard-interactive")) {
//						KBIAuthentication kbi = new KBIAuthentication();
//						kbi.setUsername(username);
//						kbi.setKBIRequestHandler(new MultiFactorKBIRequestHandler(password, null, username, hostname, port));
//                        try {authStatus = forwardedConnection.authenticate(kbi);}
//                        catch (Exception e) {
//                            String msg = getMsgPrefix() + "Failure to MFA authenticate using key pair: " + e.getMessage();
//                            log.error(msg, e);
//                            throw e;
//                        }
//					}
//				} while (authStatus != SshAuthentication.COMPLETE  &&
//                         authStatus != SshAuthentication.FAILED    &&
//                         authStatus != SshAuthentication.CANCELLED &&
//                         forwardedConnection.isConnected());
//			}
//			else
//			{
//				/**
//				 * Authenticate the user using password authentication
//				 */
//				do
//				{
//					auth = checkForPasswordOverKBI(authenticationMethods);
//
//                    try {authStatus = forwardedConnection.authenticate(auth);}
//                    catch (Exception e) {
//                        String msg = getMsgPrefix() + "Failure to authenticate using proxy: " + e.getMessage();
//                        log.error(msg, e);
//                        throw e;
//                    }
//				} while (authStatus != SshAuthentication.COMPLETE  &&
//						 authStatus != SshAuthentication.FAILED    &&
//						 authStatus != SshAuthentication.CANCELLED &&
//						 forwardedConnection.isConnected());
//			}
//
//			// Record failed authentication attempts.
//			boolean result = forwardedConnection.isAuthenticated();
//            if (!result) {
//                String msg = getMsgPrefix() + "Failed to authenticate.";
//                log.error(msg);
//            }
//            return result;
        } catch (Exception e) {
            // All exceptions have previously been caught and logged.
            // We just have to make sure all resources are cleaned up
            // and that the declared exception type is thrown.
            if (ssh2 != null) try {
                ssh2.disconnect();
            } catch (Exception ignored) {
            }
            if (forwardedConnection != null) try {
                forwardedConnection.disconnect();
            } catch (Exception ignored) {
            }
            if (transport != null) try {
                transport.close();
            } catch (Exception ignored) {
            }

            // Null out fields.
            ssh2 = null;
            forwardedConnection = null;
            auth = null;
            con = null;

            // Throw the expected exception.
            if (e instanceof RemoteExecutionException) throw (RemoteExecutionException) e;
            else throw new RemoteExecutionException(e.getMessage(), e);
        }
    }

    /**
     * Looks through the supported auth returned from the server and overrides the
     * password auth type if the server only lists keyboard-interactive. This acts
     * as a frontline check to override the default behavior and use our
     * {@link MultiFactorKBIRequestHandler}.
     *
     * @param authenticationMethods the possible auth methods
     * @return a {@link SshAuthentication} based on the ordering and existence of auth methods returned from the server.
     */
    private SshAuthentication checkForPasswordOverKBI(String[] authenticationMethods) {
        boolean kbiAuthenticationPossible = false;
        for (int i = 0; i < authenticationMethods.length; i++) {
            if (authenticationMethods[i].equals("password")) {
                return auth;
            }
            if (authenticationMethods[i].equals("keyboard-interactive")) {

                kbiAuthenticationPossible = true;
            }
        }

        if (kbiAuthenticationPossible) {
            KBIAuthentication kbi = new KBIAuthentication();

            kbi.setUsername(username);

            kbi.setKBIRequestHandler(new MultiFactorKBIRequestHandler(password, null, username, hostname, port));

            return kbi;
        }

        return auth;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.remote.RemoteSubmissionClient#runCommand(java.lang.String)
     */
    @Override
    public String runCommand(String command) throws Exception {
        Shell shell = null;
        try {
            if (log.isDebugEnabled())
                log.debug("Forking command " + command + " on " + hostname + ":" + port);

            // Get a key for the attempt mapping.
            String attemptKey = getAttemptKey(command);

            /*
             * Start a session and do basic IO
             */
            if (authenticate(getTimeoutMillis(attemptKey))) {
                if (useTunnel()) {
                    final ForwardingClient fwd = new ForwardingClient(ssh2);

                    fwd.allowX11Forwarding("localhost:0");
                    boolean remoteForwardingResponse = fwd.requestRemoteForwarding("127.0.0.1", 22,
                            hostname, port);

                    if (remoteForwardingResponse) {
                        throw new RemoteDataException("Failed to establish a remote tunnel to " +
                                proxyHost + ":" + proxyPort);
                    }

                    /*
                     * Start the users session. It also acts as a thread to service
                     * incoming channel requests for the port forwarding for both
                     * versions. Since we have a single threaded API we have to do
                     * this to send a timely response
                     */
                    SshSession session = null;
                    try {
                        session = ssh2.openSessionChannel();
                    } catch (Exception e) {
                        String msg = "Unable to open a session channel from connection: " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    try {
                        session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                    } catch (Exception e) {
                        String msg = "v100 terminal request failed (1): " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    boolean sessionStarted = false;
                    try {
                        sessionStarted = session.startShell();
                    } catch (Exception e) {
                        String msg = "Unable to start session (1): " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    /*
                     * Start local forwarding after starting the users session.
                     */
                    int randomlyChosenTunnelPort = fwd.startLocalForwardingOnRandomPort("127.0.0.1", 10, proxyHost, proxyPort);

                    /*
                     * Now that the local proxy tunnel is running, make the call to
                     * the target server through the tunnel.
                     */
                    MaverickSSHSubmissionClient proxySubmissionClient = null;
                    String proxyResponse = null;
                    try {
                        proxySubmissionClient = new MaverickSSHSubmissionClient("127.0.0.1", randomlyChosenTunnelPort, username,
                                password, null, proxyPort, publicKey, privateKey);
                        proxyResponse = proxySubmissionClient.runCommand(command);
                        return proxyResponse;
                    } catch (RemoteDataException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RemoteDataException("Failed to connect to destination server " + hostname + ":" + port, e);
                    } finally {
                        try {
                            proxySubmissionClient.close();
                        } catch (Exception ignored) {
                        }
                    }

//					if (sessionStarted) {
//					    try {shell = new Shell(forwardedConnection);}
//                            catch (Exception e) {
//                                String msg = "Unable to create new shell (1): " + e.getMessage();
//                                log.error(msg, e);
//                                throw e;
//                            }
//					}
//					else {
//					    String msg = "Failed to establish interactive shell session to "
//                                + hostname + ":" + port + " when tunneled through "
//                                + proxyHost + ":" + proxyPort;
//					    log.error(msg);
//						throw new RemoteExecutionException(msg);
//					}
                } else {
                    // Some old SSH2 servers kill the connection after the first
                    // session has closed and there are no other sessions started;
                    // so to avoid this we create the first session and dont ever use it
                    try {
                        session = ssh2.openSessionChannel();
                    } catch (Exception e) {
                        String msg = "Unable to open a session channel: " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    try {
                        session.requestPseudoTerminal("vt100", 80, 24, 0, 0);
                    } catch (Exception e) {
                        String msg = "v100 terminal request failed (2): " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    boolean sessionStarted = false;
                    try {
                        sessionStarted = session.startShell();
                    } catch (Exception e) {
                        String msg = "Unable to start session (2): " + e.getMessage();
                        log.error(msg, e);
                        throw e;
                    }

                    if (sessionStarted) {
                        try {
                            shell = new Shell(ssh2);
                        } catch (Exception e) {
                            String msg = "Unable to create new shell (2): " + e.getMessage();
                            log.error(msg, e);
                            throw e;
                        }
                    } else {
                        String msg = "Failed to establish interactive shell session to "
                                + hostname + ":" + port;
                        log.error(msg);
                        throw new RemoteExecutionException(msg);
                    }


                    // Fork the command on the remote system.  Shell is not null if we get here.
                    ShellProcess process = null;
                    long startMs = Instant.now().toEpochMilli();
                    try {
                        process = shell.executeCommand(command, true, "UTF-8");
                    } catch (Exception e) {
                        String emsg = e.getMessage();
                        String msg = "Unable to execute command \"" + command + "\" : " + emsg;
                        log.error(msg, e);

                        // Increment the number of failed attempts for this key only if the failure
                        // was a timeout.  A new mapping will be inserted and its value incremented
                        // to 1 if the key doesn't already appear in the map.  If key already exists
                        // in the map, its value is incremented.
                        if ((emsg != null) && (emsg.indexOf("connection timed out") > -1))
                            _attemptMap.computeIfAbsent(attemptKey, k -> new AtomicInteger()).incrementAndGet();

                        throw e;
                    } finally {
                        if (log.isDebugEnabled()) {
                            long stopMs = Instant.now().toEpochMilli();
                            log.debug("***** Remote command took " + (stopMs - startMs) + "ms: " + command);
                        }

                        // Remove any attempt history on success or when attempts have been exhausted.
                        if ((process != null) || isLastAttempt(attemptKey)) _attemptMap.remove(attemptKey);
                    }

                    try {
                        // no idea why, but this fails if we don't wait for 8 seconds
                        long start = System.currentTimeMillis();
                        while (process.isActive() && (System.currentTimeMillis() - start) < (Settings.MAX_REMOTE_OPERATION_TIME * 1000)) {
                            if (log.isDebugEnabled())
                                log.debug("Process has succeeded: " + process.hasSucceeded() + "\n"
                                        + "Process exit code: " + process.getExitCode() + "\n"
                                        + "Process command output: " + process.getCommandOutput());

                            Thread.sleep(1000);
                        }

                        return process.getCommandOutput();
                    } catch (Throwable t) {
                        String msg = "Failed to read response from " + hostname;
                        log.error(msg + ": " + t.getMessage());
                        throw new RemoteExecutionException(msg, t);
                    } finally {
                        try {
                            shell.exit();
                        } catch (Throwable e) {
                            String msg = "Disregarding " + e.getClass().getSimpleName() +
                                    " exception exiting SSH shell: " + e.getMessage();
                            log.error(msg);
                        }
                    }
                }
            } else {
                throw new RemoteExecutionException("Failed to authenticate to " + hostname);
            }
        } catch (RemoteExecutionException e) {
            throw e;
        } catch (Throwable t) {
            String msg = "Failed to execute command on " + hostname + " due to " +
                    t.getClass().getSimpleName() + " exception.";
            log.error(msg + ": " + t.getMessage());
            throw new RemoteExecutionException(msg, t);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        // Disconnect all communication links.
        try {
            if (ssh2 != null) ssh2.disconnect();
        } catch (Throwable t) {
            String msg = "ssh2 disconnect failure.";
            log.warn(msg, t);
        }
        try {
            if (forwardedConnection != null) forwardedConnection.disconnect();
        } catch (Throwable t) {
            String msg = "forwardedConnection disconnect failure.";
            log.warn(msg, t);
        }
        try {
            if (session != null) session.close();
        } catch (Throwable t) {
            String msg = "session close failure.";
            log.warn(msg, t);
        }

        // Null out the fields.
        ssh2 = null;
        forwardedConnection = null;
        session = null;
    }

    @Override
    public boolean canAuthentication() {
        try {
            log.debug("Verifying authentication to " + hostname + ":" + port);
            return authenticate(org.iplantc.service.transfer.Settings.STAGING_TIMEOUT_SECS * 1000);
        } catch (RemoteExecutionException e) {
            return false;
        } finally {
            close();
        }
    }

    @Override
    public String getHost() {
        return hostname;
    }

    @Override
    public int getPort() {
        return port;
    }

    private boolean useTunnel() {
        return (!StringUtils.isBlank(proxyHost));
    }

    /**
     * Create a key unique to the user, host, port and command.
     *
     * @param command the command to run on the remote host
     * @return the key
     */
    private String getAttemptKey(String command) {
        // Create a buffer large enough to hold the key.
        StringBuilder buf = new StringBuilder(command.length() + 150);
        buf.append(username);
        buf.append('@');
        buf.append(proxyHost == null ? hostname : proxyHost);
        buf.append(':');
        buf.append(proxyPort == 0 ? port : proxyPort);
        buf.append('[');
        buf.append(command);
        buf.append(']');
        return buf.toString();
    }

    /**
     * Return the number of milliseconds we allow an ssh session to remain open
     * while a remote command execute.  This is a poor man's version of dynamic
     * timeout adjustment based on the number of previous failed attempts.
     *
     * @param key the command's look up key in the attempts mapping
     * @return the number of milliseconds to wait on a remote command
     */
    private int getTimeoutMillis(String key) {
        // The floor is the base number of seconds configured at compile time.
        int baseSecs = org.iplantc.service.transfer.Settings.STAGING_TIMEOUT_SECS;

        // Determine the number failed attempts of this command have already occurred.
        AtomicInteger previousAttempts = _attemptMap.get(key);
        if (previousAttempts == null) // 1st attempt
            return baseSecs * 1000;
        else {
            // Multiply the base number of seconds by the factor for each previous
            // attempt.  Put a reasonable limit on the calculated timeout.
            for (int i = 0; i < previousAttempts.get(); i++) baseSecs *= TIMEOUT_MULTIPLIER;
            baseSecs = Math.min(baseSecs, TIMEOUT_MAX_SECS);
            return baseSecs * 1000;
        }
    }

    /**
     * Determine if the just executed attempt is the last attempt to be tried.
     * The just executed attempt is calculated by adding one to the tally of
     * previous attempts for the specified command key, if any previous attempts
     * exist.
     *
     * @param key the attempt key that identifies a command
     * @return true if all attempts have been exhausted, false otherwise
     */
    private boolean isLastAttempt(String key) {
        // No previous attempts means we haven't exhausted all attempts.
        AtomicInteger previousAttempts = _attemptMap.get(key);
        if (previousAttempts == null) return false;

        // The current attempt will be the last attempt if it matches or exceeds the
        // retry limit.  The current attempt is one more than the previous count.
        int currentAttempt = 1 + previousAttempts.get();
        if (currentAttempt >= org.iplantc.service.transfer.Settings.MAX_STAGING_RETRIES)
            return true;
        else return false;
    }

    /**
     * Error message prefix generator that captures parameters to this
     * instance when an error occurs.
     *
     * @return the prefix to a log message
     */
    private String getMsgPrefix() {
        String s = this.getClass().getSimpleName() + " [";
        s += "hostname=" + hostname;
        s += ", port=" + port;
        s += ", usename=" + username;
        s += ", password=" + (StringUtils.isBlank(password) ? "" : "***");
        s += ", proxyHost=" + proxyHost;
        s += ", proxyPort=" + proxyPort;
        s += ", publicKey=" + (StringUtils.isBlank(publicKey) ? "" : "***");
        s += ", privateKey=" + (StringUtils.isBlank(privateKey) ? "" : "***");
        s += "]: ";
        return s;
    }

    /**
     * This method initializes maverick library logging to use the agave
     * log as the ultimate target.  By default, the maverick library does
     * not log.  Setting the log level to any of the 3 supported levels
     * (ERROR, INFO, DEBUG) enables logging in the library.
     */
    private static void initializeMaverickSFTPLogger() {
        // Create the object that bridges maverick logging to agave logging.
        // Assign a logger to the maverick logger factory has the side effect
        // of enabling maverick logging.
        LoggerFactory.setInstance(MaverickSFTPLogger.getInstance());
    }
}
