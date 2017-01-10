/**
 * Created by JC Denton on 09-01-2017.
 */
public class SchedulerInfo {
    private final String ip;
    private final int port;

    public SchedulerInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
