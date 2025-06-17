package main;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ManagerHandler implements Runnable {
    private final Socket socket;
    private final MasterServer master;

    public ManagerHandler(Socket socket, MasterServer master) {
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
                if (line.startsWith("REGISTER ")) {
                    com.fasterxml.jackson.databind.JsonNode n =
                      new com.fasterxml.jackson.databind.ObjectMapper()
                        .readTree(line.substring(9));
                    WorkerInfo w = new WorkerInfo(
                        n.get("id").asText(),
                        n.get("host").asText(),
                        n.get("port").asInt()
                    );
                    master.registerWorker(w);
                    out.println("{\"status\":\"OK\"}");
                } else if (line.startsWith("REPORT ")) {
                    List<MapResult> maps =
                      master.dispatchSalesReports(line.substring(7));
                    ReduceResult rr = master.reduce(maps);
                    out.println(new com.fasterxml.jackson.databind.ObjectMapper()
                        .writeValueAsString(rr.getVentasPorKey()));
                } else {
                    out.println("ERROR: comando desconocido");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
