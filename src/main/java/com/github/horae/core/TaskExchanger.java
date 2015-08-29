package com.github.horae.core;

/**
 * Created by yanghua on 8/25/15.
 */
public interface TaskExchanger {

    void enqueue(String msgStr, String queue);

    void multiEnqueue(String[] msgStrs, String queue);

    DequeueObject dequeue(String queue, long timeoutOfSeconds);

    void ack(String msgId);

    class DequeueObject {

        private String id;
        private String body;

        public DequeueObject() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }

}
