package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.enumeration.WorkerStatus;
import edu.upenn.cis455.mapreduce.worker.WorkerContext;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

    static final long serialVersionUID = 455555001;

    Map<Integer, WorkerContext> workers = new HashMap<>();

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws java.io.IOException
    {

        if (request.getServletPath().equals("/workerstatus")) {

            WorkerContext wc = new WorkerContext(request);
            workers.put(wc.getPort(), wc);
            // TODO ? set last updated status time / last received

        } else if (request.getServletPath().equals("/status")) {
        }


        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("<html><head><title>Master</title></head>");
        out.println("<body>Hi, I am the master!</body></html>");
    }
}
  
