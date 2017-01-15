package TurboFramework.InformationObjects;

public class SchedulerInfo {
    private final String ip;
    private final int port;
    private int heartFrequency;

    public SchedulerInfo(String ip, int port, int heartFrequency) {
        this.ip = ip;
        this.port = port;
        this.heartFrequency = heartFrequency;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getHeartFrequency() {
        return heartFrequency;
    }
}
