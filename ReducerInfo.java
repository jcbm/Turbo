import java.io.Serializable;

/**
 * Created by JC Denton on 09-01-2017.
 */
public class ReducerInfo implements Serializable {
    private final String ip;
    private final int port;
    private final String guid;

    public ReducerInfo(String ip, int port, String GUID) {

        this.ip = ip;
        this.port = port;
        this.guid = GUID;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getGuid() {
        return guid;
    }
}
