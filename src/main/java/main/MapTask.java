package main;



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Objects;

/**
 * Encapsula un trabajo de filtrado que enviamos a un Worker.
 */
public class MapTask {
    private final FilterSpec filters;
    private final WorkerInfo targetWorker;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public MapTask(FilterSpec filters, WorkerInfo targetWorker) {
        this.filters = Objects.requireNonNull(filters);
        this.targetWorker = Objects.requireNonNull(targetWorker);
    }

    /**
     * Envía el FilterSpec al Worker, lee su MapResult y lo deserializa.
     */
    public MapResult execute() throws IOException {
        try (Socket sock = new Socket(targetWorker.getHost(), targetWorker.getPort());
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()))) {

            // 1. Construimos el mensaje TASK con el filtro en JSON
            String filterJson;
            try {
                filterJson = MAPPER.writeValueAsString(filters);
            } catch (JsonProcessingException e) {
                throw new IOException("Error serializando FilterSpec", e);
            }
            Message request = new Message(Message.MessageType.TASK, filterJson);
            out.println(request.toJson());

            // 2. Leemos la respuesta
            String responseLine = in.readLine();
            if (responseLine == null) {
                throw new IOException("Conexión cerrada inesperadamente por " + targetWorker);
            }
            Message response = Message.fromJson(responseLine);

            // 3. Validamos que sea un RESULT y deserializamos el MapResult
            if (response.getType() != Message.MessageType.RESULT) {
                throw new IOException("Esperado RESULT, recibido " + response.getType());
            }
            try {
                return MAPPER.readValue(response.getPayload(), MapResult.class);
            } catch (JsonProcessingException e) {
                throw new IOException("Error parseando MapResult JSON", e);
            }
        }
    }

    public WorkerInfo getTargetWorker() {
        return targetWorker;
    }
}
