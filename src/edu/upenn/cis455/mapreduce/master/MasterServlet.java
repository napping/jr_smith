package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;
import edu.upenn.cis455.mapreduce.utils.HtmlStrings;
import edu.upenn.cis455.mapreduce.worker.WorkerContext;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

    static final long serialVersionUID = 455555001;

    Map<String, WorkerContext> workers = new HashMap<>();

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {
        switch (request.getServletPath()) {

            case ("/workerstatus"):
                WorkerContext wc = new WorkerContext(request);
                workers.put(wc.getJob(), wc);

                // TODO should I print anything to out response?

                break;

            case ("/status"):
                PrintWriter writer = response.getWriter();
                writer.println(HtmlStrings.generatePrettyHTMLHeader());
                writer.println(HtmlStrings.generatePrettyTableHeader());

                for (Map.Entry<String, WorkerContext> e : workers.entrySet()) {
                    if (new Date().getTime() - e.getValue().getLastUpdated().getTime() < 30000) {
                        WorkerContext context = e.getValue();

                        writer.println("<tr>");

                        writer.println("<td>");
                        writer.println(context.getPort());
                        writer.println("</td>");

                        writer.println("<td>");
                        writer.println(context.getWorkerStatus());
                        writer.println("</td>");

                        writer.println("<td>");
                        writer.println(context.getJob());
                        writer.println("</td>");

                        writer.println("<td>");
                        writer.println(context.getKeysRead());
                        writer.println("</td>");

                        writer.println("<td>");
                        writer.println(context.getKeysWritten());
                        writer.println("</td>");

                        writer.println("<td>");
                        writer.println(context.getLastUpdated());
                        writer.println("</td>");

                        writer.println("<td>");
                        writer.println();
                        writer.println("</td>");

                        writer.println("</tr>");

                    } else {
                        workers.remove(e);
                        System.out.println("Worker at port + " + e.getKey() + " removed from " +
                                "workers.");
                    }
                }

                writer.println(HtmlStrings.generatePrettyTableFooter());


                writer.println(HtmlStrings.generatePrettyHTMLFooter());

                break;

            default:
                // Error

        }

        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("<html><head><title>Master</title></head>");
        out.println("<body>Hi, I am the master!</body></html>");
    }
}
  
