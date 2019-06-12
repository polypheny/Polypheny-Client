package ch.unibas.dmi.dbis.polyphenydb.client.job;


import ch.unibas.dmi.dbis.polyphenydb.client.job.analysis.PolyphenyCdlAnalysis;
import ch.unibas.dmi.dbis.polyphenydb.client.job.evaluation.PolyphenyCdlEvaluation;
import java.util.NoSuchElementException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * {@link PolyphenyJobCdl} represents the XML in java-format
 *
 * @author Silvan Heller
 */
@XmlRootElement(name = "chronos")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyJobCdl {

    @XmlElement(name = "evaluation", nillable = true)
    private PolyphenyCdlEvaluation evaluation = new PolyphenyCdlEvaluation();

    @XmlElement(name = "analysis", nillable = true)
    private PolyphenyCdlAnalysis analysis = new PolyphenyCdlAnalysis();


    public PolyphenyCdlEvaluation getEvaluation() throws NoSuchElementException {
        if ( evaluation == null ) {
            throw new NoSuchElementException( "evaluation == null" );
        }
        return evaluation;
    }


    public void setEvaluation( PolyphenyCdlEvaluation evaluation ) {
        this.evaluation = evaluation;
    }


    public PolyphenyCdlAnalysis getAnalysis() throws NoSuchElementException {
        if ( analysis == null ) {
            throw new NoSuchElementException( "analysis == null" );
        }
        return analysis;
    }


    public void setAnalysis( PolyphenyCdlAnalysis analysis ) {
        this.analysis = analysis;
    }


    @Override
    public String toString() {
        return "PolyphenyJobCdl{\n" +
                "evaluation=" + evaluation +
                ",\n analysis=" + analysis +
                "\n}";
    }
}
