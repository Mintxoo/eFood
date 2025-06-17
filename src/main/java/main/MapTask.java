package main;

import java.io.*;
import java.net.Socket;
import java.util.Objects;

public class MapTask {
    private final FilterSpec filters;
    private final WorkerInfo targetWorker;
    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper();

    public MapTask(FilterSpec filters, WorkerInfo targetWorker) {
        this.filters = Objects.requireNonNull(filters);
        this.targetWorker = Objects.requireNonNull(targetWorker);
    }

    public MapResult execute() throws IOException {
        try (Socket sock = new Socket(targetWorker.getHost(), targetWorker.getPort());
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()))) {

            String jsonFiltros = MAPPER.writeValueAsString(filters);
            Message req = new Message(Message.MessageType.TASK, jsonFiltros);
            out.println(req.toJson());

            String line = in.readLine();
            if (line == null) throw new IOException("Worker cerró: " + targetWorker);
            Message resp = Message.fromJson(line);
            if (resp.getType() != Message.MessageType.RESULT) {
                throw new IOException("Esperado RESULT, vino " + resp.getType());
            }
            return MAPPER.readValue(resp.getPayload(), MapResult.class);
        } catch (Exception e) {
            throw new IOException("MapTask error en " + targetWorker, e);
        }
    }
}
