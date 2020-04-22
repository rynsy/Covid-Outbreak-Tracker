package cs505final.database;

public class DatabaseSubscriber {

    private String topic;

    public DatabaseSubscriber(String topic, String streamName) {
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
