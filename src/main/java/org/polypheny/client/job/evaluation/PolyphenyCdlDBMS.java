package org.polypheny.client.job.evaluation;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import org.polypheny.client.config.Config;


/**
 * Stores access information for the DBMS to be benchmarked
 *
 * @author Silvan Heller
 */
@XmlRootElement(name = "dbms")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyCdlDBMS {

    @XmlAttribute(name = "host")
    private String host = Config.DEFAULT_HOST;

    @XmlAttribute(name = "port")
    private int port = Config.DEFAULT_DBMS_PORT;

    @XmlAttribute(name = "database")
    private String database = Config.DEFAULT_DATABASE_NAME;
    @XmlAttribute(name = "username")
    private String username = Config.DEFAULT_DBMS_USERNAME;
    @XmlAttribute(name = "password")
    private String password = Config.DEFAULT_DBMS_PASSWORD;


    @Override
    public String toString() {
        return "PolyphenyCdlDBMS{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }


    public String getDatabase() {
        return database;
    }


    public String getUsername() {
        return username;
    }


    public String getPassword() {
        return password;
    }


    public String getHost() {
        return host;
    }


    public void setHost( String host ) {
        this.host = host;
    }


    public int getPort() {
        return port;
    }


    public void setPort( int port ) {
        this.port = port;
    }


}
