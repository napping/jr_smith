package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;
import edu.upenn.cis455.mapreduce.utils.MyHttpURLConnection;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {
        switch (request.getRequestURI()) {

            case "/runmap":
                context.setStatus(WorkerStatus.MAPPING);
                context.setJob(request.getParameter("job"));
                try {
                    jobInstance = (Job) Class.forName(context.getJob()).newInstance();

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();

                } catch (InstantiationException e) {
                    e.printStackTrace();

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                this.inputDir = request.getParameter("input");
                this.numMapThreads = Integer.parseInt(request.getParameter("numThreads"));

                this.numWorkers = Integer.parseInt(request.getParameter("numWorkers"));

                for (int i = 0; i < this.numWorkers; i++) {
                    String name = "worker" + i;
                    this.workerAddresses.put(name, request.getParameter(name));
                }

                break;

            case "/runreduce":
                // TODO
                break;

            default:
                // TODO
        }
    }
}

























