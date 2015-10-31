package edu.upenn.cis455.mapreduce.utils;

import edu.upenn.cis455.mapreduce.worker.WorkerContext;

/**
 * @author brishi
 */
public class HtmlStrings {

    public static String generatePrettyHTMLHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("<html>");
        sb.append("<head>");
        sb.append("<title>Status</title>");
        sb.append("<link rel='stylesheet' href='http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css'/>");
        sb.append("</head>");
        sb.append("<body>");
        sb.append("<div class='container'>");
        sb.append("<h1>The Status Page</h1>");

        return sb.toString();
     }

    public static String generatePrettyHTMLFooter() {
        StringBuilder sb = new StringBuilder();

        sb.append("</div>");
        sb.append("</body>");
        sb.append("</html>");

        return sb.toString();
    }

    public static String generatePrettyTableHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("<div>");
        sb.append("<h3>Worker Information</h3>");
        sb.append("<table>");
        sb.append("<tr>");

        sb.append("<th>Port");
        sb.append("</th>");

        sb.append("<th>Status");
        sb.append("</th>");

        sb.append("<th>Keys Read");
        sb.append("</th>");

        sb.append("<th>Keys Written");
        sb.append("</th>");

        sb.append("<th>Last Updated");
        sb.append("</th>");

        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("</tr>");

        return sb.toString();
    }

    public static String generatePrettyTableFooter() {
        StringBuilder sb = new StringBuilder();
        sb.append("</table>");
        sb.append("<div>");
        return sb.toString();
    }

    public static String generatePrettyWebForm() {
        StringBuilder sb = new StringBuilder();
        sb.append("<div>");
        sb.append("<h3>");
        sb.append("Submit Jobs");
        sb.append("</h3>");
        sb.append("<form action='/submitJob' method='POST'>");

        sb.append("<h5>Class Name of the Job</h5>");
        sb.append("<input type='text' name='className'>");

        sb.append("<h5>Input Directory</h5>");
        sb.append("<input type='text' name='inputDirectory'>");

        sb.append("<h5>Output Directory</h5>");
        sb.append("<input type='text' name='outputDirectory'>");

        sb.append("<h5>Number of Map Threads on Each Worker</h5>");
        sb.append("<input type='text' name='numMap'>");

        sb.append("<h5>Number of Reduce Threads on Each Worker</h5>");
        sb.append("<input type='text' name='numReduce'>");

        sb.append("<input type='submit' value='Submit'>");

        sb.append("</form>");
        sb.append("</div>");
        return sb.toString();
    }

    public static String generatePrettyTableRows(WorkerContext context) {
        StringBuilder sb = new StringBuilder();

        sb.append("<tr>");

        sb.append("<td>");
        sb.append(context.getPort());
        sb.append("</td>");

        sb.append("<td>");
        sb.append(context.getWorkerStatus());
        sb.append("</td>");

        sb.append("<td>");
        sb.append(context.getJob());
        sb.append("</td>");

        sb.append("<td>");
        sb.append(context.getKeysRead());
        sb.append("</td>");

        sb.append("<td>");
        sb.append(context.getKeysWritten());
        sb.append("</td>");

        sb.append("<td>");
        sb.append(context.getLastUpdated());
        sb.append("</td>");

        sb.append("</tr>");

        return sb.toString();
    }
}
