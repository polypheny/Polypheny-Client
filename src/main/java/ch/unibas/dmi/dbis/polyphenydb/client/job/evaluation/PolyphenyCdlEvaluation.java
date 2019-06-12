package ch.unibas.dmi.dbis.polyphenydb.client.job.evaluation;


import ch.unibas.dmi.dbis.polyphenydb.client.config.Config;
import ch.unibas.dmi.dbis.polyphenydb.client.job.setup.PolyphenyCdlSetup;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * Stores evaluation information. This should as few fields as possible.
 *
 * @author Silvan Heller
 */
@XmlRootElement(name = "evaluation")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyCdlEvaluation {

    @XmlElement(name = "setup")
    private PolyphenyCdlSetup setup = new PolyphenyCdlSetup();
    @XmlAttribute(name = "system", required = true)
    private String chronosSystem = Config.DEFAULT_CHRONOS_SYSTEM;
    @XmlElement(name = "dbms", required = true)
    private PolyphenyCdlDBMS dbms = new PolyphenyCdlDBMS();
    @XmlElement(name = "parameters", required = true)
    private PolyphenyCdlParams params = new PolyphenyCdlParams();
    @XmlElement(name = "options", required = true)
    private PolyphenyCdlOptions options = new PolyphenyCdlOptions();


    public PolyphenyCdlSetup getSetup() {
        return setup;
    }


    public PolyphenyCdlOptions getOptions() {
        return options;
    }


    public void setOptions( PolyphenyCdlOptions options ) {
        this.options = options;
    }


    public PolyphenyCdlParams getParams() {
        return params;
    }


    public void setParams( PolyphenyCdlParams params ) {
        this.params = params;
    }


    public String getChronosSystem() {
        return chronosSystem;
    }


    public void setChronosSystem( String chronosSystem ) {
        this.chronosSystem = chronosSystem;
    }


    public PolyphenyCdlDBMS getDbms() {
        return dbms;
    }


    public void setDbms( PolyphenyCdlDBMS dbms ) {
        this.dbms = dbms;
    }


    @Override
    public String toString() {
        return "PolyphenyCdlEvaluation{" +
                "setup=" + setup +
                ", chronosSystem='" + chronosSystem + '\'' +
                ", dbms=" + dbms +
                ", params=" + params +
                ", options=" + options +
                '}';
    }
}
