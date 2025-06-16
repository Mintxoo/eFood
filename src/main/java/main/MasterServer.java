package main;



import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MasterServer {
    private final int port;
    private ServerSocket serverSocket;
    private final List<WorkerInfo> workerNodes = new ArrayList<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public MasterServer(int port) { this.port = port; }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("MasterServer listening on port " + port);
        while (!serverSocket.isClosed()) {
            Socket sock = serverSocket.accept();
            // Para ejemplificar, interpretamos REGISTER vs SEARCH/BUY por prefijo
            sock.setSoTimeout(2000);
            var peek = new java.io.BufferedReader(
                          new java.io.InputStreamReader(sock.getInputStream()))
                          .readLine();
            if (peek != null && peek.startsWith("REGISTER")) {
                new Thread(new ManagerHandler(sock, this)).start();
            } else {
                new Thread(new ClientHandler(sock, this)).start();
            }
        }
    }

    public void stop() throws IOException {
        serverSocket.close();
    }

    public synchronized void registerWorker(WorkerInfo info) {
        workerNodes.add(info);
        System.out.println("Registered worker: " + info);
    }

    /** Envía un mensaje a TODOS los workers (por ejemplo, ventas). */
    public void broadcast(Message msg) {
        for (WorkerInfo w : workerNodes) {
            try (Socket sock = new Socket(w.getHost(), w.getPort());
                 PrintWriter out = new PrintWriter(sock.getOutputStream(), true)) {
                out.println(msg.toJson());
            } catch (Exception e) {
                System.err.println("Error al hacer broadcast a " + w + ": " + e);
            }
        }
    }

    /** Mapea el filtro a cada worker y devuelve todos los MapResult. */
    public List<MapResult> dispatchMapTasks(FilterSpec filters) {
        List<MapResult> partials = new ArrayList<>();
        for (WorkerInfo w : workerNodes) {
            try {
                partials.add(new MapTask(filters, w).execute());
            } catch (IOException e) {
                System.err.println("MapTask falló en " + w + ": " + e);
            }
        }
        return partials;
    }

    /** Solicita a cada worker su reporte de ventas y combina resultados. */
    public List<MapResult> dispatchSalesReports(String reportType) {
        List<MapResult> partials = new ArrayList<>();
        for (WorkerInfo w : workerNodes) {
            try {
                partials.add(new ReportTask(reportType, w).execute());
            } catch (IOException e) {
                System.err.println("ReportTask falló en " + w + ": " + e);
            }
        }
        return partials;
    }

    /** Reduce sobre los MapResult dados. */
    public ReduceResult reduce(List<MapResult> partials) {
        return new ReduceTask().combine(partials);
    }

    public static void main(String[] args) throws IOException {
        int port = 5555;
        new MasterServer(port).start();
    }
}
