package TurboFramework.InformationObjects;

import TurboFramework.Enums.NodeType;

import java.util.Date;

public class DateNodeTypePair {
    private final Date lastHeartBeat;
    private final NodeType nodeType;

    public DateNodeTypePair(Date lastHeartBeat, NodeType nodeType) {
        this.lastHeartBeat = lastHeartBeat;
        this.nodeType = nodeType;
    }

    public Date getLastHeartBeat() {
        return lastHeartBeat;
    }

    public NodeType getNodeType() {
        return nodeType;
    }
}
