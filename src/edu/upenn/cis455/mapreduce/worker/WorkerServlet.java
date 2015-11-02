package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;
import edu.upenn.cis455.mapreduce.utils.MyHttpURLConnection;
import edu.upenn.cis455.mapreduce.utils.Utils;

import java.io.*;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {

    static final long serialVersionUID = 455555002;

    String master;
    String storageDir;
    WorkerContext context;

    String inputDir;
    String outputDir;

    int numMapThreads;
    int numReduceThreads;
    int numWorkers;
    private Map<String, String> workerAddresses;
    Job jobInstance;

    Context mapContext;
    Context reduceContext;

    LinkedList<BufferedReader> allReaders;

    List<BufferedWriter> spoolOutWriters;

    List<Thread> mapThreads;
    List<Thread> reduceThreads;

    File spoolIn;
    File spoolOut;

    BigInteger workerRange;

    BufferedReader readReduce;

    public void init() {
        ServletConfig config = getServletConfig();

        context = new WorkerContext();
        master = config.getInitParameter("master");
        storageDir = config.getInitParameter("storagedir");
        context.setPort(Integer.parseInt(config.getInitParameter("port")));
        workerAddresses = new HashMap<>();

        mapThreads = new LinkedList<>();
        reduceThreads = new LinkedList<>();

        Thread statusThread = new StatusThread(true);
        statusThread.start();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("<html><head><title>Worker</title></head>");
        out.println("<body>Hi, I am the worker!</body></html>");
    }


    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {
        switch (request.getRequestURI()) {

            case "/runmap":
                prepareMap(request);
                startMapThreads();

                sendPushData();

                for (BufferedWriter bw : spoolOutWriters) {
                    bw.close();
                }

                new StatusThread(false).start();

                break;

            case "/runreduce":
                context.setJob(request.getParameter("job"));

                try {
                    jobInstance = (Job) Class.forName(context.getJob()).newInstance();
                } catch (InstantiationException |
                        IllegalAccessException | ClassNotFoundException e) {
                    e.printStackTrace();
                }

                spoolIn = new File(storageDir + "spool-in");

                outputDir = request.getParameter("output"); // TODO File o = mkdirHelper(storageDirectory + outputDirectory);

                numReduceThreads = Integer.parseInt(request.getParameter("numThreads"));
                File reduceMe = new File(storageDir + "/spool-in/reduceMe");

                try {
                    Runtime.getRuntime().exec(
                            "sort -o " +
                                    reduceMe.getAbsolutePath() +
                                    " " +
                                    reduceMe.getAbsolutePath()
                    ).waitFor();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                readReduce = new BufferedReader(new FileReader(reduceMe));

                BufferedWriter reduceWriter = new BufferedWriter(new FileWriter(
                        new File(storageDir+outputDir+"/mapreduce_results")));

                reduceContext = new ReduceContext(reduceWriter);

                for (int i = 1; i <= numReduceThreads; i++) {
                   reduceThreads.add(new ReduceThread());
                }

                for (Thread t : reduceThreads) {
                    t.start();
                }

                for (Thread t: reduceThreads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }


                /*synchronized (worker) {
                    worker.setStatus(Status.IDLE);
                }*/
                reduceWriter.close();

                break;

            case "/pushdata":
                BufferedReader reader = request.getReader();

                if (spoolIn == null) {
                    spoolIn = new File(storageDir + "spool-in");
                }

                File spoolIn = new File(storageDir + "spool-in/reduceMe");
                BufferedWriter writer = new BufferedWriter(new FileWriter(spoolIn, true));

                String line = "";
                while ((line = reader.readLine()) != null) {
                    writer.write(line + "\n");
                }

                reader.close();
                writer.close();

            default:
                // TODO
        }
    }

    public class StatusThread extends Thread {

        private boolean repeat = true;
        public StatusThread(boolean repeat) {
            this.repeat = repeat;

        }

        public void run() {
            while (true) {
                try {
                    MyHttpURLConnection conn = new MyHttpURLConnection(
                            new URL("http://" + master + "/workerstatus"));

                    conn.setParam("port", context.getPort());
                    conn.setParam("status", context.getStatus());
                    conn.setParam("job", context.getJob());
                    conn.setParam("keysRead", context.getKeysRead());
                    conn.setParam("keysWritten", context.getKeysWritten());

                    conn.sendGetRequest();

                    if (!repeat) {
                        break;
                    }

                    Thread.sleep(10000);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void prepareMap(HttpServletRequest request) throws FileNotFoundException {
        context.setStatus(WorkerStatus.MAPPING);
        context.setJob(request.getParameter("job"));
        try {
            jobInstance = (Job) Class.forName(context.getJob()).newInstance();

        } catch (ClassNotFoundException | InstantiationException |
                IllegalAccessException e) {
            e.printStackTrace();
        }

        this.inputDir = request.getParameter("input");
        this.numMapThreads = Integer.parseInt(request.getParameter("numThreads"));
        this.numWorkers = Integer.parseInt(request.getParameter("numWorkers"));

        for (int i = 0; i < this.numWorkers; i++) {
            String name = "worker" + i;
            this.workerAddresses.put(name, request.getParameter(name));
        }

        this.workerRange = new BigInteger("2").pow(160).divide(
                new BigInteger(Integer.toString(numWorkers)));

        this.mapContext = new MapContext();

        this.allReaders = Utils.getFileReaders(
                new File(storageDir + inputDir));

        makeSpoolDirs();

        this.spoolOutWriters = Utils.getFileWriters(numWorkers, storageDir);
    }

    public class MapThread extends Thread {
        public void run() {
            while (true) {

                String line = null;

                synchronized (allReaders) {
                    BufferedReader reader = allReaders.peekFirst();
                    try {
                        line = reader.readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (line == null) {
                        if (allReaders.isEmpty()) {
                            break;
                        }
                        allReaders.pollFirst();

                    } else {
                        context.setKeysRead(context.getKeysRead() + 1);
                    }
                }

                if (line != null) {
                    String[] keyValue = line.split("\t");
                    jobInstance.map(keyValue[0], keyValue[1], mapContext);
                }
            }
        }
    }

    String currKey;
    public class ReduceThread extends Thread {
        Map<String, List<String>> keyValues;
        String prevLine = "";
        String key = "";

        // Terribly hardcoded
        public void run() {
            try {
                while (true) {
                    String line;
                    synchronized (readReduce) {
                        if (currKey == null) {
                            if ((line = readReduce.readLine()) == null) {
                                break;
                            }
                            synchronized (context) {
                                context.setKeysRead(context.getKeysRead() + 1);
                            }

                            Tuple t = new Tuple(line);
                            currKey = t.key;

                            if (keyValues.containsKey(t.key)) {
                                keyValues.get(currKey).add(t.value);
                            } else {
                                keyValues.put(currKey, new ArrayList<String>());
                                keyValues.get(currKey).add(t.value);
                            }

                        }

                        while ((line = readReduce.readLine()) != null) {
                            synchronized (context) {
                                context.setKeysRead(context.getKeysRead() + 1);
                            }

                            Tuple t = new Tuple(line);
                            if (currKey.equals(t.key)) {
                                if (keyValues.containsKey(t.key)) {
                                    keyValues.get(currKey).add(t.value);
                                } else {
                                    keyValues.put(currKey, new ArrayList<String>());
                                    keyValues.get(currKey).add(t.value);
                                }
                            } else {

                            }


                        }



                        if (keyValues == null) {
                            if (prevLine == "")
                                prevLine = readReduce.readLine();
                            if (prevLine == null)
                                break;
                            kv = new Tuple(prevLine);
                        }

                        while (((line = readReduce.readLine()) != null)
                                && (key = line.split("\t")[0].trim())
                                .equals(kv.getKey())) {
                            kv.addValue(line.split("\t")[1].trim());
                            synchronized (context) {
                                context.setKeysRead(context.getKeysRead() + 1);
                            }
                        }
                    }
                    System.out.println("Key: " + kv.getKey()+" Values: "+kv.getValues());
                    String[] values = new String[kv.getValues().size()];
                    kv.getValues().toArray(values);
                    myJob.reduce(kv.getKey(), values, reduceContext);

                    prevLine = line;
                    kv = null;
                    key = "";
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void sendPushData() {
        for (File f : spoolOut.listFiles()) {
            if (f.isFile() && f.getName().charAt(0) != '.') {
                try {
                    MyHttpURLConnection conn = new MyHttpURLConnection(new URL(
                            "http://" + workerAddresses.get(f.getName()) + "/pushdata"));

                    int statusCode = conn.sendPostFileRequest(f);
                    if (statusCode == 200) {
                        context.setStatus(WorkerStatus.WAITING);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class MapContext implements Context {
        @Override
        public void write(String key, String value) {
            MessageDigest sha1 = null;
            try {
                sha1 = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            sha1.update(key.getBytes());
            BigInteger hash = new BigInteger(1, sha1.digest());

            int workerId = hash.divide(workerRange).intValue();

            if (workerId > numWorkers - 1) {
                workerId = numWorkers - 1;
            }

            BufferedWriter br = spoolOutWriters.get(workerId);

            try {
                br.write(key + "\t" + value + "\n");
                context.setKeysWritten(context.getKeysWritten() + 1);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public class ReduceContext implements Context {
        BufferedWriter writer;
        public ReduceContext(BufferedWriter reduceWriter) {
            this.writer = reduceWriter;
        }

        @Override
        public void write(String key, String value) {
            // TODO
        }
    }

    private void makeSpoolDirs() {
        this.spoolIn = new File(this.storageDir + "/spool-in");
        this.spoolOut = new File(this.storageDir + "/spool-out");

        Utils.newDir(this.spoolIn);
        Utils.newDir(this.spoolOut);
    }

    private void startMapThreads() {
        for (int i = 0; i < this.numMapThreads; i++) {
            Thread mapThread = new WorkerServlet.MapThread();
            this.mapThreads.add(mapThread);
            mapThread.start();
        }

        for (Thread t : mapThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public class Tuple {
        public String key;
        public String value;

        public Tuple(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public Tuple(String kvString) {
            String key = "";
            String value = "";
            parseKeyValue(kvString);
        }

        public void parseKeyValue(String kvString) {
            // There could be tabs inside the value string...
            for (int i = 0; i < kvString.length(); i++) {
                char c = kvString.charAt(i);
                if (c != '\t') {
                    key += c;
                } else {
                    value = kvString.substring(i + 1);
                    break;
                }
            }
        }

    }
}


























