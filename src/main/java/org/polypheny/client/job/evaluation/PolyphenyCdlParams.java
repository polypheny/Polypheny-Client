package org.polypheny.client.job.evaluation;


import java.util.Arrays;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import org.polypheny.client.config.Config;
import org.polypheny.client.job.PolyphenyJobCdl;


/**
 * Stores parameters of a {@link PolyphenyJobCdl}
 *
 * @author Silvan Heller
 */
@XmlRootElement(name = "parameters")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyCdlParams {

    @XmlAttribute(name = "workers", required = true)
    private int workers = Config.DEFAULT_WORKERS;

    @XmlAttribute(name = "worker_url", required = true)
    private String worker_url = Config.DEFAULT_WORKER_URL;

    @XmlAttribute(name = "measurement_time", required = true)
    private int measurementTime = Config.MEASUREMENT_TIME_MIN;


    @Override
    public String toString() {
        return "PolyphenyCdlParams{" +
                "workers=" + workers +
                ", worker_url='" + Arrays.toString( getWorkerURLs() ) + '\'' +
                ", measurementTime=" + measurementTime +
                '}';
    }


    /**
     * @return measurement time in miliseconds
     */
    public int getMeasurementTime() {
        return measurementTime * 60_000;
    }


    public void setMeasurementTime( int measurementTime ) {
        this.measurementTime = measurementTime;
    }


    public int getMeasurementTimeInMinutes() {
        return measurementTime;
    }


    /**
     * Returns urls of format url:port. If the port is not specified in the XML, uses {@link Config#DEFAULT_WORKER_PORT}
     */
    public String[] getWorkerURLs() {
        String[] split = worker_url.split( "," );
        String[] urls = new String[split.length];
        for ( int i = 0; i < split.length; i++ ) {
            String[] _url = split[i].split( ":" );
            if ( _url.length == 1 ) {
                urls[i] = _url[0] + ":" + Config.DEFAULT_WORKER_PORT;
                continue;
            }
            if ( _url.length == 2 ) {
                urls[i] = split[i];
                continue;
            }
            throw new IllegalArgumentException( "URL " + split[i] + " has an invalid format" );
        }
        return urls;
    }


    public int getWorkers() {
        return workers;
    }


    public void setWorkers( int workers ) {
        this.workers = workers;
    }


    public void setWorker_url( String worker_url ) {
        this.worker_url = worker_url;
    }


}
