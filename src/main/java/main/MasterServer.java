// src/main/java/main/MasterServer.java
package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MasterServer {
    private final int port;
    private ServerSocket serverSocket;
    private final List<WorkerInfo> workers = new ArrayList<>();

    public MasterServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("MasterServer escuchando en puerto " + port);

        while (!serverSocket.isClosed()) {
            Socket sock = serverSocket.accept();
            try {
                // Abrimos el ObjectInputStream para leer el primer Message
                ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                Message firstMsg = (Message) ois.readObject();

                // Segun el tipo, arrancamos el handler correspondiente
                if (firstMsg.getType() == Message.MessageType.REGISTER) {
                    // Manager se registra o solicita REPORT
                    new Thread(new ManagerHandler(sock, this, firstMsg, ois))
                        .start();
                } else {
                    // Cliente normal (PING, TASK, SALE, etc.)
                    new Thread(new ClientHandler(sock, this, firstMsg, ois))
                        .start();
                }
            } catch (ClassNotFoundException | IOException e) {
                // Si hay error leyendo el primer mensaje, cerramos socket
                System.err.println("Error al aceptar conexión: " + e.getMessage());
                sock.close();
            }
        }
    }

    /** Registra un nuevo worker en la lista compartida. */
    public synchronized void registerWorker(WorkerInfo wi) {
        workers.add(wi);
        System.out.println("Worker registrado: " + wi);
    }

    /** Envía un Message a todos los workers conectados. */
    public void broadcast(Message msg) {
        for (WorkerInfo w : workers) {
            try (Socket s = new Socket(w.getHost(), w.getPort());
                 java.io.ObjectOutputStream oos =
                     new java.io.ObjectOutputStream(s.getOutputStream())) {
                oos.writeObject(msg);
                oos.flush();
            } catch (Exception e) {
                System.err.println("Broadcast falló en " + w + ": " + e);
            }
        }
    }

    /** Envía FilterSpec a todos los workers y recoge los MapResult parciales. */
    public List<MapResult> dispatchMapTasks(FilterSpec fs) throws ClassNotFoundException {
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

    /** Envía petición de reporte a todos los workers y recoge MapResult. */
    public List<MapResult> dispatchSalesReports(String type) throws ClassNotFoundException {
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

    /** Combina todos los MapResult en un solo ReduceResult. */
    public ReduceResult reduce(List<MapResult> partials) {
        return new ReduceTask().combine(partials);
    }

    /** Cierra el servidor. */
    public void stop() throws IOException {
        serverSocket.close();
    }

    public static void main(String[] args) throws IOException {
        int port = 5555;
        new MasterServer(port).start();
    }
}
