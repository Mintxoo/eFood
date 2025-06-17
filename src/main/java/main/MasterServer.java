package main;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MasterServer {
    private final int port;
    private ServerSocket serverSocket;
    private final List<WorkerInfo> workers = new ArrayList<>();
    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper();

    public MasterServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("MasterServer escuchando en " + port);
        while (!serverSocket.isClosed()) {
            Socket sock = serverSocket.accept();
            BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String peek = br.readLine();
            if (peek != null && peek.startsWith("REGISTER ")) {
                new Thread(new ManagerHandler(sock, this)).start();
            } else {
                new Thread(new ClientHandler(sock, this, peek)).start();
            }
        }
    }

    public synchronized void registerWorker(WorkerInfo wi) {
        workers.add(wi);
        System.out.println("Worker registrado: " + wi);
    }

    public void broadcast(Message msg) {
        for (WorkerInfo w : workers) {
            try (Socket s = new Socket(w.getHost(), w.getPort());
                 PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
                out.println(msg.toJson());
            } catch (Exception e) {
                System.err.println("Broadcast fallo en " + w + ": " + e);
            }
        }
    }

    public List<MapResult> dispatchMapTasks(FilterSpec fs) {
        List<MapResult> res = new ArrayList<>();
        for (WorkerInfo w : workers) {
            try {
                res.add(new MapTask(fs, w).execute());
            } catch (IOException e) {
                System.err.println("MapTask falló en " + w + ": " + e);
            }
        }
        return res;
    }

    public List<MapResult> dispatchSalesReports(String type) {
        List<MapResult> res = new ArrayList<>();
        for (WorkerInfo w : workers) {
            try {
                res.add(new ReportTask(type, w).execute());
            } catch (IOException e) {
                System.err.println("ReportTask falló en " + w + ": " + e);
            }
        }
        return res;
    }

    public ReduceResult reduce(List<MapResult> partials) {
        return new ReduceTask().combine(partials);
    }

    public void stop() throws IOException {
        serverSocket.close();
    }

    public static void main(String[] args) throws IOException {
        new MasterServer(5555).start();
    }
}
