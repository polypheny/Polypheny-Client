package ch.unibas.dmi.dbis.polyphenydb.client.job.setup;


import ch.unibas.dmi.dbis.polyphenydb.client.config.Config;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * Setup
 */
@XmlRootElement(name = "setup")
@XmlAccessorType(XmlAccessType.FIELD)
public class PolyphenyCdlSetup {

    @XmlAttribute(name = "populate")
    private boolean populate = Config.POPULATE_DATABASE;

    @XmlAttribute(name = "create_schema")
    private boolean createSchema = Config.CREATE_SCHEMA;

    @XmlAttribute(name = "output_folder_path")
    private String outputFolderPath = Config.DEFAULT_OUTPUT_FOLDER;

    @XmlAttribute(name = "input_folder_path")
    private String inputFolderPath = Config.DEFAULT_INPUT_FOLDER;


    public boolean getCreateSchema() {
        return createSchema;
    }


    public void setCreateSchema( boolean createSchema ) {
        this.createSchema = createSchema;
    }


    public boolean getPopulate() {
        return populate;
    }


    public void setPopulate( boolean populate ) {
        this.populate = populate;
    }


    public String getOutputFolderPath() {
        return outputFolderPath;
    }


    public void setOutputFolderPath( String outputFolderPath ) {
        this.outputFolderPath = outputFolderPath;
    }


    public String getInputFolderPath() {
        return inputFolderPath;
    }


    public void setInputFolderPath( String inputFolderPath ) {
        this.inputFolderPath = inputFolderPath;
    }


    @Override
    public String toString() {
        return "PolyphenyCdlSetup{" +
                "populate=" + populate +
                ", createSchema=" + createSchema +
                ", outputFolderPath='" + outputFolderPath + '\'' +
                ", inputFolderPath='" + inputFolderPath + '\'' +
                '}';
    }
}
