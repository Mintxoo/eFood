package main;

public class Message {
    public enum MessageType { PING, PONG, TASK, RESULT, REGISTER, SALE, REPORT }

    private MessageType type;
    private String payload;

    public Message() {}

    public Message(MessageType type, String payload) {
        this.type = type;
        this.payload = payload;
    }

    public MessageType getType() { return type; }
    public String getPayload() { return payload; }

    public String toJson() throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(this);
    }

    public static Message fromJson(String json) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(json, Message.class);
    }
}
