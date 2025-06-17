package main;

import java.io.*;
import java.net.Socket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final MasterServer master;
    private final String firstLine;

    public ClientHandler(Socket socket, MasterServer master, String firstLine) {
        this.socket = socket;
        this.master = master;
        this.firstLine = firstLine;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(
                 new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(
                 socket.getOutputStream(), true)) {

            String line = firstLine;
            do {
                if (line.startsWith("SEARCH ")) {
                    handleSearch(line.substring(7), out);
                } else if (line.startsWith("BUY ")) {
                    master.broadcast(new Message(Message.MessageType.SALE, line.substring(4)));
                    out.println("{\"status\":\"OK\"}");
                } else {
                    out.println("ERROR: comando desconocido");
                }
            } while ((line = in.readLine()) != null);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleSearch(String jsonParams, PrintWriter out) {
        try {
            com.fasterxml.jackson.databind.JsonNode n =
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(jsonParams);
            FilterSpec fs = new FilterSpec(
                n.get("latitude").asDouble(),
                n.get("longitude").asDouble(),
                jsonArrayToSet(n.withArray("foodCategories")),
                n.get("minStars").asInt(),
                PriceCategory.valueOf(n.get("priceCategory").asText())
            );
            List<MapResult> maps = master.dispatchMapTasks(fs);
            ReduceResult rr = master.reduce(maps);
            out.println(new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(rr));
        } catch (Exception e) {
            out.println("ERROR en SEARCH: " + e.getMessage());
        }
    }

    private Set<String> jsonArrayToSet(com.fasterxml.jackson.databind.JsonNode arr) {
        Set<String> s = new HashSet<>();
        Iterator<com.fasterxml.jackson.databind.JsonNode> it = arr.elements();
        while (it.hasNext()) s.add(it.next().asText());
        return s;
    }
}
