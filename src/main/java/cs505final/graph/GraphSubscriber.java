package cs505final.graph;

public class GraphSubscriber {

    private String topic;

    public GraphSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");
            //String[] sstr = String.valueOf(msg).split(":");
            //String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Long.parseLong(outval[0]);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    public String getTopic() {
        return topic;
    }

}
