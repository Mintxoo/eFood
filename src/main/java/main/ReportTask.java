package main;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Envía al Worker una petición de reporte de ventas (food o product).
 */
public class ReportTask {
    private final String reportType;
    private final WorkerInfo targetWorker;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ReportTask(String reportType, WorkerInfo targetWorker) {
        this.reportType = reportType;
        this.targetWorker = targetWorker;
    }

    /**
     * Envía un Message(REPORT, reportType) y lee el MapResult devuelto.
     */
    public MapResult execute() throws IOException {
        try (Socket sock = new Socket(targetWorker.getHost(), targetWorker.getPort());
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()))) {

            Message request = new Message(Message.MessageType.REPORT, reportType);
            out.println(request.toJson());

            String line = in.readLine();
            if (line == null) throw new IOException("Worker cerró antes de responder: " + targetWorker);
            Message resp = Message.fromJson(line);
            if (resp.getType() != Message.MessageType.RESULT) {
                throw new IOException("Esperado RESULT en reporte, llegó " + resp.getType());
            }
            return MAPPER.readValue(resp.getPayload(), MapResult.class);
        } catch (Exception e) {
            throw new IOException("Error en ReportTask hacia " + targetWorker, e);
        }
    }
}
