// src/main/java/main/ClientHandler.java
package main;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClientHandler implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;

    private final Socket socket;
    private final MasterServer master;
    private final ObjectInputStream ois;
    private final ObjectOutputStream oos;
    private final Message firstMsg;
    private final boolean hasInitial;

    /** Constructor original (sin peek). */
    public ClientHandler(Socket socket, MasterServer master) throws IOException {
        this.socket = socket;
        this.master = master;
        this.oos = new ObjectOutputStream(socket.getOutputStream());
        this.ois = new ObjectInputStream(socket.getInputStream());
        this.firstMsg = null;
        this.hasInitial = false;
    }

    /** Constructor con peek: recibe primer Message y su ObjectInputStream. */
    public ClientHandler(Socket socket,
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
            // Si viene mensaje inicial, lo procesamos primero
            if (hasInitial) {
                process(firstMsg);
            }

            // Bucle de lectura normal
            Message msg;
            while ((msg = (Message) ois.readObject()) != null) {
                process(msg);
            }

        } catch (EOFException eof) {
            // cliente cerró conexión
        } catch (Exception e) {
            System.err.println("Error en ClientHandler: " + e);
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void process(Message msg) throws IOException, ClassNotFoundException {
        switch (msg.getType()) {
            case PING -> {
                oos.writeObject(new Message(Message.MessageType.PONG, "OK"));
            }
            case TASK -> {
                FilterSpec fs = (FilterSpec) msg.getPayload();
                List<MapResult> partials = master.dispatchMapTasks(fs);
                ReduceResult rr = new ReduceTask().combine(partials);
                oos.writeObject(new Message(Message.MessageType.RESULT, rr));
            }
            case SALE -> {
                // reenviamos la venta a todos los Workers
                master.broadcast(new Message(Message.MessageType.SALE, msg.getPayload()));
                oos.writeObject(new Message(Message.MessageType.RESULT, "OK"));
            }
            default -> {
                oos.writeObject(new Message(
                    Message.MessageType.RESULT,
                    "ERROR: comando no soportado por ClientHandler"
                ));
            }
        }
        oos.flush();
    }
}
