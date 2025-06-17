package main;

import java.io.*;
import java.net.Socket;

public class ReportTask {
    private final String reportType;
    private final WorkerInfo targetWorker;
    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper();

    public ReportTask(String reportType, WorkerInfo targetWorker) {
        this.reportType = reportType;
        this.targetWorker = targetWorker;
    }

    public MapResult execute() throws IOException {
        try (Socket sock = new Socket(targetWorker.getHost(), targetWorker.getPort());
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()))) {

            Message req = new Message(Message.MessageType.REPORT, reportType);
            out.println(req.toJson());

            String line = in.readLine();
            if (line == null) throw new IOException("Worker cerró: " + targetWorker);
            Message resp = Message.fromJson(line);
            if (resp.getType() != Message.MessageType.RESULT) {
                throw new IOException("Esperado RESULT, vino " + resp.getType());
            }
            return MAPPER.readValue(resp.getPayload(), MapResult.class);
        } catch (Exception e) {
            throw new IOException("ReportTask error en " + targetWorker, e);
        }
    }
}
