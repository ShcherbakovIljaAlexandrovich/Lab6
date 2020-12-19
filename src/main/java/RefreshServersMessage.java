import java.util.ArrayList;

public class RefreshServersMessage {
    private final ArrayList<String> servers;

    public ArrayList<String> getServers(){
        return servers;
    }

    public RefreshServersMessage(ArrayList<String> servers) {
        this.servers = servers;
    }
}
