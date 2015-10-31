package edu.upenn.cis455.mapreduce.utils;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;
import java.util.StringJoiner;

/**
 * @author brishi
 */
public class MyHttpURLConnection {
    private URL url;
    StringJoiner paramSJ;

    public MyHttpURLConnection (URL url) {
        this.url = url;
        paramSJ = new StringJoiner("&");
    }

    public void setParam(String key, Object value) {
        paramSJ.add(key);
        paramSJ.add("=");
        paramSJ.add(value.toString());
    }

    // TODO
    public void sendGetRequest() throws IOException {
        String paramString = "?" + paramSJ.toString();

        Socket s = new Socket(url.getHost(), url.getPort());
        PrintWriter pw = new PrintWriter(s.getOutputStream());

        pw.print("GET / HTTP/1.1");
        pw.print("Host: stackoverflow.com");
        pw.flush();
        BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
        String t;
        while((t = br.readLine()) != null) System.out.println(t);
        br.close();



    }

    // TODO
    public void sendPostRequest() {
        String bodyString = paramSJ.toString();
    }
}

