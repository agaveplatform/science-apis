package org.agaveplatform.service.transfers.nats;

import io.nats.client.Options;
import io.nats.client.impl.Headers;
import org.agaveplatform.service.transfers.nats.NatsArgs.Builder;

public class NatsArgs {
    public enum Trail {MESSAGE, COUNT, QUEUE_AND_COUNT}

    public String server = Options.DEFAULT_URL;
    public String subject;
    public String queue;
    public String message;
    public int msgCount = -1;
    public int subCount = -1;
    public String stream = null;
    public String mirror = null;
    public String consumer = null;
    public String durable = null;
    public String deliver = null;
    public int pullSize = 0;
    public Headers headers;
    public String user = null;
    public String pass = null;
    public boolean containedUnknown = false;

    public boolean hasHeaders() {
        return headers != null && headers.size() > 0;
    }

    public NatsArgs(String[] args, Trail trail, String usageString) {
        try {
            if (args != null) {
                String lastKey = null;
                for (int x = 0; x < args.length; x++) {
                    String arg = args[x];
                    if (arg.startsWith("-")) {
                        if (++x >= args.length) {
                            usage(usageString);
                        }
                        handleKeyedArg(arg, args[x]);
                        lastKey = arg;
                    }
                    else if (trail == null) {
                        if (lastKey != null) {
                            handleKeyedArg(lastKey, arg);
                        }
                    }
                    else {
                        handleTrailingArg(trail, arg);
                        lastKey = null;
                    }
                }
            }
        }
        catch (RuntimeException e) {
            System.err.println("Exception while processing command line arguments: " + e + "\n");
            usage(usageString);
        }
    }

    private void handleTrailingArg(Trail trail, String arg) {
        if (subject == null) { // subject always the first expected
            subject = arg;
        }
        else if (trail == Trail.MESSAGE) {
            if (message == null) {
                message = arg;
            }
            else {
                message = message + " " + arg;
            }
        }
        else if (trail == Trail.QUEUE_AND_COUNT) {
            if (queue == null) {
                queue = arg;
            }
            else {
                msgCount = Integer.parseInt(arg);
            }
        }
        else { // Expect.COUNT
            msgCount = Integer.parseInt(arg);
        }
    }

    private void handleKeyedArg(String key, String value) {
        switch (key) {
            case "-s":
                server = value;
                break;
            case "-sub":
                subject = value;
                break;
            case "-q":
                queue = value;
                break;
            case "-m":
                if (message == null) {
                    message = value;
                }
                else {
                    message = message + " " + value;
                }
                break;
            case "-con":
                consumer = value;
                break;
            case "-strm":
                stream = value;
                break;
            case "-mir":
                mirror = value;
                break;
            case "-pull":
                pullSize = Integer.parseInt(value);
                break;
            case "-mcnt":
                msgCount = Integer.parseInt(value);
                break;
            case "-scnt":
                subCount = Integer.parseInt(value);
                break;
            case "-dur":
                durable = value;
                break;
            case "-dlvr":
                deliver = value;
                break;
            case "-user":
                user = value;
                break;
            case "-pass":
                pass = value;
                break;
            case "-r":
                if (headers == null) {
                    headers = new Headers();
                }
                String[] hdr = value.split(":");
                headers.add(hdr[0], hdr[1]);
                break;
            default:
                containedUnknown = true;
                break;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String subjectDefault;
        private String queueDefault;
        private String messageDefault;
        private int msgCountDefault = -1;
        public int subCountDefault = -1;
        private String streamDefault = null;
        private String mirrorDefault = null;
        private String consumerDefault = null;
        private String durableDefault = null;
        private String deliverDefault = null;
        private int pullSizeDefault = 0;
        private String userDefault = null;
        private String passDefault = null;
        private boolean uniqueify = false;

        public Builder defaultSubject(String subject) {
            this.subjectDefault = subject;
            return this;
        }

        public Builder defaultQueue(String queue) {
            this.queueDefault = queue;
            return this;
        }

        public Builder defaultMessage(String message) {
            this.messageDefault = message;
            return this;
        }

        public Builder defaultMsgCount(int msgCount) {
            this.msgCountDefault = msgCount;
            return this;
        }

        public Builder defaultSubCount(int subCount) {
            this.subCountDefault = subCount;
            return this;
        }

        public Builder defaultStream(String stream) {
            this.streamDefault = stream;
            return this;
        }

        public Builder defaultMirror(String mirror) {
            this.mirrorDefault = mirror;
            return this;
        }

        public Builder defaultConsumer(String consumer) {
            this.consumerDefault = consumer;
            return this;
        }

        public Builder defaultDurable(String durable) {
            this.durableDefault = durable;
            return this;
        }

        public Builder defaultDeliver(String deliver) {
            this.deliverDefault = deliver;
            return this;
        }

        public Builder defaultUser(String user) {
            this.userDefault = user;
            return this;
        }

        public Builder defaultPass(String pass) {
            this.passDefault = pass;
            return this;
        }

        public Builder defaultPullSize(int pullSize) {
            this.pullSizeDefault = pullSize;
            return this;
        }

        public Builder uniqueify() {
            uniqueify = true;
            return this;
        }

        public NatsArgs build(String[] args, String usageString) {
            NatsArgs ea = new NatsArgs(args, null, usageString);
            if (ea.containedUnknown && usageString != null) {
                usage(usageString);
            }
            if (ea.subject == null) {
                ea.subject = subjectDefault;
            }
            if (ea.queue == null) {
                ea.queue = queueDefault;
            }
            if (ea.message == null) {
                ea.message = messageDefault;
            }
            if (ea.msgCount == -1) {
                ea.msgCount = msgCountDefault;
            }
            if (ea.subCount == -1) {
                ea.subCount = subCountDefault;
            }
            if (ea.stream == null) {
                ea.stream = streamDefault;
            }
            if (ea.mirror == null) {
                ea.mirror = mirrorDefault;
            }
            if (ea.consumer == null) {
                ea.consumer = consumerDefault;
            }
            if (ea.durable == null) {
                ea.durable = durableDefault;
            }
            if (ea.deliver == null) {
                ea.deliver = deliverDefault;
            }
            if (ea.user == null) {
                ea.user = userDefault;
            }
            if (ea.pass == null) {
                ea.pass = passDefault;
            }
            if (ea.pullSize == 0) {
                ea.pullSize = pullSizeDefault;
            }
            if (uniqueify) {
                String u = "-" + uniqueEnough();
                if (ea.stream != null) {
                    ea.stream += u;
                }
                if (ea.mirror != null) {
                    ea.mirror += u;
                }
                if (ea.subject != null) {
                    ea.subject += u;
                }
                if (ea.queue != null) {
                    ea.queue += u;
                }
                if (ea.consumer != null) {
                    ea.consumer += u;
                }
                if (ea.durable != null) {
                    ea.durable += u;
                }
                if (ea.deliver != null) {
                    ea.deliver += u;
                }
            }
            return ea;
        }

        private String uniqueEnough() {
            String hex = Long.toHexString(System.currentTimeMillis()).substring(6);
            StringBuilder sb = new StringBuilder();
            for (int x = 0; x < hex.length(); x++) {
                char c = hex.charAt(x);
                if (c < 58) {
                    sb.append((char)(c+55));
                }
                else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }
    }

    private static void usage(String usageString) {
        if (usageString != null) {
            System.out.println(usageString);
        }
        System.exit(-1);
    }
}

