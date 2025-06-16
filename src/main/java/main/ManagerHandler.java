package main;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.Socket;
import java.util.List;

/**
 * Atiende REGISTER (workers) y REPORT (ventas).
 */
public class ManagerHandler implements Runnable {
    private final Socket socket;
    private final MasterServer master;
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
                    handleAddWorker(line.substring(9), out);
                } else if (line.startsWith("REPORT ")) {
                    handleReportSales(line.substring(7), out);
                } else {
                    out.println("ERROR: comando desconocido");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleAddWorker(String workerJson, PrintWriter out) {
        try {
            JsonNode n = MAPPER.readTree(workerJson);
            WorkerInfo w = new WorkerInfo(
                n.get("id").asText(),
                n.get("host").asText(),
                n.get("port").asInt()
            );
            master.registerWorker(w);
            out.println("{\"status\":\"OK\"}");
        } catch (Exception e) {
            out.println("ERROR en REGISTER: " + e.getMessage());
        }
    }

    private void handleReportSales(String reportType, PrintWriter out) {
        try {
            // reportType debe ser "food" o "product"
            List<MapResult> maps = master.dispatchSalesReports(reportType);
            ReduceResult r = master.reduce(maps);
            out.println(MAPPER.writeValueAsString(r.getVentasPorKey()));
        } catch (Exception e) {
            out.println("ERROR en REPORT: " + e.getMessage());
        }
    }
}
