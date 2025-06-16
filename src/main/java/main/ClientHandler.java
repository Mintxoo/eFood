package main;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.au.ds.model.PriceCategory;

import java.io.*;
import java.net.Socket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Atiende SEARCH y BUY desde el cliente.
 */
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final MasterServer master;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ClientHandler(Socket socket, MasterServer master) {
        this.socket = socket;
        this.master = master;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(
                 new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(
                 socket.getOutputStream(), true)) {

            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("SEARCH ")) {
                    handleSearch(line.substring(7), out);
                } else if (line.startsWith("BUY ")) {
                    handleBuy(line.substring(4), out);
                } else {
                    out.println("ERROR: comando desconocido");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleSearch(String jsonParams, PrintWriter out) {
        try {
            JsonNode node = MAPPER.readTree(jsonParams);
            double lat = node.get("latitude").asDouble();
            double lon = node.get("longitude").asDouble();
            int stars = node.get("minStars").asInt();
            PriceCategory pc = PriceCategory.valueOf(node.get("priceCategory").asText());
            Set<String> cats = new HashSet<>();
            Iterator<JsonNode> it = node.withArray("foodCategories").elements();
            while (it.hasNext()) cats.add(it.next().asText());

            FilterSpec filters = new FilterSpec(lat, lon, cats, stars, pc);
            List<MapResult> maps = master.dispatchMapTasks(filters);
            ReduceResult result = master.reduce(maps);
            out.println(MAPPER.writeValueAsString(result));
        } catch (Exception e) {
            out.println("ERROR en SEARCH: " + e.getMessage());
        }
    }

    private void handleBuy(String saleJson, PrintWriter out) {
        try {
            // reenviamos el JSON de la venta a todos los workers
            Message msg = new Message(Message.MessageType.SALE, saleJson);
            master.broadcast(msg);
            out.println("{\"status\":\"OK\"}");
        } catch (Exception e) {
            out.println("ERROR en BUY: " + e.getMessage());
        }
    }
}

