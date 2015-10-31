package edu.upenn.cis455.mapreduce.utils;


import java.io.*;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.util.StringJoiner;

/**
 * @author brishi
 */
public class MyHttpURLConnection {
    private URL url;
    StringJoiner paramSJ;
    Socket socket;

    public MyHttpURLConnection (URL url) throws IOException {
        this.url = url;
        paramSJ = new StringJoiner("&");

        this.socket = new Socket(url.getHost(), url.getPort());
    }

    public void setParam(String key, Object value) {
        paramSJ.add(key);
        paramSJ.add("=");
        paramSJ.add(value.toString());
    }

    public void sendGetRequest() throws IOException {
        String paramString = "?" + paramSJ.toString();

        PrintWriter writer = new PrintWriter(socket.getOutputStream());

        writer.println("GET" + url.getPath() + paramString + " HTTP/1.1");
        writer.println("Host: " + url.getHost());
        writer.println();
        writer.flush();

        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String t;
        while ((t = br.readLine()) != null) {
            System.out.println(t);
        }
        System.out.println("Please verify the above is a 200 ^.");

        br.close();
    }

    public void sendPostRequest() throws IOException {
        String bodyString = paramSJ.toString();

        BufferedWriter out = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), "UTF8"));

        out.write("POST " + url.getPath() + " HTTP/1.1\r\n");
        out.write("Content-Length: " + bodyString.getBytes().length + "\r\n");
        out.write("Content-Type: application/x-www-form-urlencoded\r\n");
        out.write("\r\n");

        out.write(bodyString);
        out.flush();

        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println("Please verify the above is a 200 ^.");

        out.close();
        in.close();
    }

    public void sendPostFileRequest(File f) throws IOException {
        byte[] data = Files.readAllBytes(f.toPath());
        OutputStream out = socket.getOutputStream();
        byte[] line1 = ("POST " + url.getPath() + " HTTP/1.1\r\n").getBytes();
        byte[] line2 = ("Content-Length: " + data.length + "\r\n").getBytes();
        byte[] line3 = "Content-Type: application/x-www-form-urlencoded\r\n".getBytes();
        byte[] line4 = "\r\n".getBytes();

        out.write(line1);
        out.write(line2);
        out.write(line3);
        out.write(line4);

        out.write(data);

    }
}
