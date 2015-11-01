package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;
import edu.upenn.cis455.mapreduce.utils.MyHttpURLConnection;
import edu.upenn.cis455.mapreduce.utils.Utils;

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
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

    public void init() {
        ServletConfig config = getServletConfig();

        context = new WorkerContext();
        master = config.getInitParameter("master");
        storageDir = config.getInitParameter("storagedir");
        context.setPort(Integer.parseInt(config.getInitParameter("port")));
        workerAddresses = new HashMap<>();

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

                BigInteger sha1 = new BigInteger("2").pow(160).divide(
                        new BigInteger(Integer.toString(numWorkers)));

                this.mapContext = new MapContext();

                this.allReaders = Utils.getFileReaders(
                        new File(storageDir + inputDir));

                break;

            case "/runreduce":
                // TODO
                break;

            default:
                // TODO
        }
    }

    public class StatusThread extends Thread {
        boolean repeat = true;
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

    public class MapContext implements Context {
        @Override
        public void write(String key, String value) {
            // TODO
        }
    }

    public class ReduceContext implements Context {
        @Override
        public void write(String key, String value) {
            // TODO
        }
    }
}


























