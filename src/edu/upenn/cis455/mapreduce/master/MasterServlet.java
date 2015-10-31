package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;
import edu.upenn.cis455.mapreduce.utils.HtmlStrings;
import edu.upenn.cis455.mapreduce.utils.MyHttpURLConnection;
import edu.upenn.cis455.mapreduce.worker.WorkerContext;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

    static final long serialVersionUID = 455555001;

    Map<String, WorkerContext> workers = new HashMap<>();
    String className;
    String inputDirectory;
    String outputDirectory;
    int numMap;
    int numReduce;

    int numWorkers = workers.size();

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {
        switch (request.getRequestURI()) {

            case ("/workerstatus"):
                WorkerContext wc = new WorkerContext(request);
                workers.put(wc.getIpAddress() + ":" + wc.getPort(), wc);

                if (this.readyReduce()) {
                    this.runReduce();
                }

                break;

            case ("/status"):
                PrintWriter writer = response.getWriter();
                writer.println(HtmlStrings.generatePrettyHTMLHeader());
                writer.println(HtmlStrings.generatePrettyTableHeader());

                synchronized (workers) {
                    for (Map.Entry<String, WorkerContext> e : workers.entrySet()) {
                        if (new Date().getTime() - e.getValue().getLastUpdated().getTime() < 30000) {
                            WorkerContext context = e.getValue();
                            writer.println(HtmlStrings.generatePrettyTableRows(context));

                        } else {
                            workers.remove(e);
                            System.out.println("Worker at IP address + " + e.getKey() + " removed " +
                                    "from workers.");
                        }
                    }
                }

                writer.println(HtmlStrings.generatePrettyTableFooter());
                writer.println(HtmlStrings.generatePrettyWebForm());
                writer.println(HtmlStrings.generatePrettyHTMLFooter());

                break;

            default:
                response.setContentType("text/html");
                PrintWriter out = response.getWriter();
                out.println("<html><head><title>Master</title></head>");
                out.println("<body>Hi, I am the master!</body></html>");
        }
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {

        if (request.getRequestURI().equals("/submitJob")) {
            className = request.getParameter("className");
            inputDirectory = request.getParameter("inputDirectory");
            outputDirectory = request.getParameter("outputDirectory");
            numMap = Integer.parseInt(request.getParameter("numMap"));
            numReduce = Integer.parseInt(request.getParameter("numReduce"));

            this.runMap();
        }
    }

    private void runMap() throws MalformedURLException {
        int numWorkers = workers.size();
        String[] workersIPPort = new String[numWorkers];

        synchronized (workers) {
            int i = 0;
            for (String ipPort : workers.keySet()) {
                workersIPPort[i] = ipPort;
                i++;
            }
        }

        for (String s : workers.keySet()) {
            URL workerUrl = new URL("http://" + s + "/runmap");

            MyHttpURLConnection conn = new MyHttpURLConnection(workerUrl);
            conn.setParam("job", className);
            conn.setParam("input", inputDirectory);
            conn.setParam("numThreads", numMap);
            conn.setParam("numWorkers", numWorkers);

            for (int j = 0; j < workersIPPort.length; j++) {
                conn.setParam("worker" + j, workersIPPort[j]);
            }

            conn.sendPostRequest();
        }
    }

    private boolean readyReduce() {
        synchronized (workers) {
            for (Map.Entry<String, WorkerContext> e : workers.entrySet()) {
                WorkerContext context = e.getValue();
                if (new Date().getTime() - context.getLastUpdated().getTime() < 30000) {
                    if (context.getWorkerStatus() != WorkerStatus.WAITING) {
                        return false;

                    } else {
                        workers.remove(e);
                    }
                }
            }
        }

        return true;
    }

    private void runReduce() throws MalformedURLException {
        for (String s : workers.keySet()) {
            URL workerUrl = new URL("http://" + s + "/runmap");

            MyHttpURLConnection conn = new MyHttpURLConnection(workerUrl);
            conn.setParam("job", className);
            conn.setParam("output", outputDirectory);
            conn.setParam("numThreads", numReduce);

            conn.sendPostRequest();
        }

    }
}











