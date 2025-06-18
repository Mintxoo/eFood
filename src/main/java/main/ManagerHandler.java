// src/main/java/main/ManagerHandler.java
package main;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class ManagerHandler implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;

    private final Socket socket;
    private final MasterServer master;
    private final ObjectInputStream ois;
    private final ObjectOutputStream oos;
    private final Message firstMsg;
    private final boolean hasInitial;

    /** Constructor original (sin peek). */
    public ManagerHandler(Socket socket, MasterServer master) throws IOException {
        this.socket = socket;
        this.master = master;
        this.oos = new ObjectOutputStream(socket.getOutputStream());
        this.ois = new ObjectInputStream(socket.getInputStream());
        this.firstMsg = null;
        this.hasInitial = false;
    }

    /** Constructor con peek: recibe primer Message y su ObjectInputStream. */
    public ManagerHandler(Socket socket,
                          MasterServer master,
                          Message firstMsg,
                          ObjectInputStream ois) throws IOException {
        this.socket = socket;
        this.master = master;
        this.oos = new ObjectOutputStream(socket.getOutputStream());
        this.ois = ois;
        this.firstMsg = firstMsg;
        this.hasInitial = true;
    }

    @Override
    public void run() {
        try {
            // Si vino mensaje inicial, lo procesamos primero
            if (hasInitial) {
                process(firstMsg);
            }

            // Bucle de lectura normal
            Message msg;
            while ((msg = (Message) ois.readObject()) != null) {
                process(msg);
            }

        } catch (EOFException eof) {
            // Manager cerró la conexión
        } catch (Exception e) {
            System.err.println("Error en ManagerHandler: " + e);
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void process(Message msg) throws IOException, ClassNotFoundException {
        switch (msg.getType()) {
            case REGISTER -> {
                WorkerInfo w = (WorkerInfo) msg.getPayload();
                master.registerWorker(w);
                oos.writeObject(new Message(Message.MessageType.RESULT, "OK"));
            }
            case REPORT -> {
                String reportType = (String) msg.getPayload();
                List<MapResult> partials = master.dispatchSalesReports(reportType);
                ReduceResult rr = master.reduce(partials);
                Map<String,Integer> ventas = rr.getVentasPorKey();
                oos.writeObject(new Message(Message.MessageType.RESULT, ventas));
            }
            default -> {
                oos.writeObject(new Message(
                    Message.MessageType.RESULT,
                    "ERROR: comando no soportado por ManagerHandler"
                ));
            }
        }
        oos.flush();
    }
}
