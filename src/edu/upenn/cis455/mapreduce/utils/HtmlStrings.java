package edu.upenn.cis455.mapreduce.utils;

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

        return sb.toString();
     }

    public static String generatePrettyHTMLFooter() {
        StringBuilder sb = new StringBuilder();

        sb.append("</body>");
        sb.append("</html>");

        return sb.toString();
    }

    public static String generatePrettyTableHeader() {
        StringBuilder sb = new StringBuilder();

        sb.append("<body>");
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
        return sb.toString();
    }

}
