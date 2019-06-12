package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects;


import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.Utils;
import java.sql.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class NewOrder {

    private static final Logger logger = LogManager.getLogger();
    private Integer NO_O_ID;
    private Integer NO_D_ID;
    private Integer NO_W_ID;


    public NewOrder() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public NewOrder( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public NewOrder( Integer NO_O_ID, Integer NO_D_ID, Integer NO_W_ID ) {
        this.NO_O_ID = NO_O_ID;
        this.NO_D_ID = NO_D_ID;
        this.NO_W_ID = NO_W_ID;
    }


    public Integer getNO_O_ID() {
        return NO_O_ID;
    }


    public void setNO_O_ID( Integer NO_O_ID ) {
        this.NO_O_ID = NO_O_ID;
    }


    public Integer getNO_D_ID() {
        return NO_D_ID;
    }


    public void setNO_D_ID( Integer NO_D_ID ) {
        this.NO_D_ID = NO_D_ID;
    }


    public Integer getNO_W_ID() {
        return NO_W_ID;
    }


    public void setNO_W_ID( Integer NO_W_ID ) {
        this.NO_W_ID = NO_W_ID;
    }


    @Override
    public String toString() {
        return "NewOrder{" +
                "NO_O_ID=" + NO_O_ID +
                ", NO_D_ID=" + NO_D_ID +
                ", NO_W_ID=" + NO_W_ID +
                '}';
    }
}
