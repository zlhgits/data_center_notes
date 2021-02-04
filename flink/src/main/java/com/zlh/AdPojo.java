package com.zlh;

/**
 * @package com.zlh
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/9/1
 */
public class AdPojo {
    private String itemid;
    private String timestamp;
    private String value;
    private String host;
    private String ip;
    private String systemName;

    public String getHost() {
        return host;
    }

    public String getIp() {
        return ip;
    }

    public String getItemid() {
        return itemid;
    }

    public String getSystemName() {
        return systemName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setItemid(String itemid) {
        this.itemid = itemid;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return itemid+"  "+timestamp+"  "+value+"  "+host+"  "+ip+"  "+systemName;
    }
}
