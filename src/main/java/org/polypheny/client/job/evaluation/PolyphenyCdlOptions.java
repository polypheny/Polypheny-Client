package org.polypheny.client.job.evaluation;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.config.Config;
import org.polypheny.client.config.MusqleConfig;
import org.polypheny.client.config.TPCHConfig;
import org.polypheny.client.grpc.PolyClientGRPC.AccessMethod;
import org.polypheny.client.grpc.PolyClientGRPC.DBMSSystem;
import org.polypheny.client.grpc.PolyClientGRPC.Scenario;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
@XmlRootElement(name = "options")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyCdlOptions {

    private static Logger logger = LogManager.getLogger();

    @XmlAttribute(name = "accessmethod", required = true)
    private String accessMethod = Config.DEFAULT_ACCESS_METHOD;

    @XmlAttribute(name = "scenario", required = true)
    private String scenario = Config.DEFAULT_SCENARIO;

    @XmlAttribute(name = "tpcc_warehouses")
    private int tpccWarehouses = Config.DEFAULT_TPCC_WAREHOUSES;

    @XmlAttribute(name = "tpcc_terminal_think")
    private boolean tpccTerminalThink = Config.TPCC_TERMINAL_THINK;

    @XmlAttribute(name = "tpch_scalefactor")
    private double tpchScalefactor = Config.TPCH_SCALEFACTOR;

    @XmlAttribute(name = "tpch_streams")
    private int tpchStreams = TPCHConfig.STREAMS;

    @XmlAttribute(name = "musqle_streams")
    private int musqleStreams = MusqleConfig.STREAMS;

    @XmlAttribute(name = "system", required = true)
    private String system = Config.DEFAULT_DBMS_SYSTEM;

    @XmlAttribute(name = "netdata_measurements")
    private String measurements = "";

    @XmlAttribute(name = "ycsb_properties_path")
    private String ycsbPropertiesPath = Config.DEFAULT_YCSB_PROPERTIES_PATH;


    @Override
    public String toString() {
        return "PolyphenyCdlOptions{" +
                "accessMethod='" + getAccessMethod() + '\'' +
                ", scenario='" + getScenario() + '\'' +
                ", tpccWarehouses=" + tpccWarehouses +
                ", tpccTerminalThink=" + tpccTerminalThink +
                ", tpchScalefactor=" + tpchScalefactor +
                ", tpchStreams=" + tpchStreams +
                ", musqleStreams=" + musqleStreams +
                ",\n ycsbProperties=" + getYcsbProperties() +
                ",\n system='" + getSystem() + '\'' +
                ", measurements='" + getMeasurementOptions() + '\'' +
                '}';
    }


    public Properties getYcsbProperties() {
        try {
            Properties properties = new Properties();
            properties.load( this.getClass().getResourceAsStream( "/" + ycsbPropertiesPath ) );
            return properties;
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * https://stackoverflow.com/questions/17209260/converting-java-util-properties-to-hashmapstring-string
     */
    public Map<String, String> getYcsbPropertiesMap() {
        Properties properties = getYcsbProperties();
        Map<String, String> map = new HashMap<>();
        for ( final String name : properties.stringPropertyNames() ) {
            map.put( name, properties.getProperty( name ) );
        }
        return map;
    }


    /**
     * We expect the string we get from the XML to be equal to the Protobuf name
     */
    public List<String> getMeasurementOptions() {
        if ( measurements.equals( "" ) ) {
            return Config.DEFAULT_NETDATA_MEASUREMENTS;
        }
        ArrayList<String> measurementOptions = new ArrayList<>();
        measurementOptions.addAll( Arrays.asList( measurements.split( "," ) ) );
        return measurementOptions;
    }


    public boolean getTpccTerminalThink() {
        return tpccTerminalThink;
    }


    public void setTpccTerminalThink( boolean tpccTerminalThink ) {
        this.tpccTerminalThink = tpccTerminalThink;
    }


    public double getTpchScalefactor() {
        return tpchScalefactor;
    }


    public void setTpchScalefactor( double tpchScalefactor ) {
        this.tpchScalefactor = tpchScalefactor;
    }


    public Scenario getScenario() {
        return Scenario.valueOf( scenario );
    }


    public void setScenario( String scenario ) {
        this.scenario = scenario;
    }


    /**
     * Parses the String for an Access Method. Accepts {@link AccessMethod#toString()}
     */
    public AccessMethod getAccessMethod() {
        return AccessMethod.valueOf( accessMethod );

    }


    public void setAccessMethod( String accessMethod ) {
        this.accessMethod = accessMethod;
    }


    /**
     * Parses the String for a DBMS System. Accepts {@link DBMSSystem#toString()}
     */
    public DBMSSystem getSystem() {
        return DBMSSystem.valueOf( system );
    }


    public void setSystem( String system ) {
        this.system = system;
    }


    public int getTpccWarehouses() {
        return tpccWarehouses;
    }


    public void setTpccWarehouses( int tpccWarehouses ) {
        this.tpccWarehouses = tpccWarehouses;
    }


    public int getTpchStreams() {
        return tpchStreams;
    }


    public int getMusqleStreams() {
        return musqleStreams;
    }


}
