package org.polypheny.client.job.analysis;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "analysis")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyCdlAnalysis {

    @Override
    public String toString() {
        return "PolyphenyCdlAnalysis{}";
    }
    //We have no analysis parameters
}
