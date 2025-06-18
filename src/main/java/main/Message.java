// src/main/java/main/Message.java
package main;

import java.io.Serializable;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum MessageType { PING, PONG, TASK, RESULT, REGISTER, SALE, REPORT }

    private MessageType type;
    private Object payload;  // Ahora puede ser String, FilterSpec, MapResult, etc.

    public Message() {}

    public Message(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    public MessageType getType() {
        return type;
    }

    public Object getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message{" +
               "type=" + type +
               ", payload=" + payload +
               '}';
    }
}
