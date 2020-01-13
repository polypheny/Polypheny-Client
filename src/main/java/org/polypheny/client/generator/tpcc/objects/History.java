package org.polypheny.client.generator.tpcc.objects;


import java.sql.ResultSet;
import java.sql.Timestamp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.generator.tpcc.Utils;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class History {

    private static final Logger logger = LogManager.getLogger();
    private Timestamp H_DATE;
    private Double H_AMOUNT;
    private String H_DATA;
    private Integer H_C_ID;
    private Integer H_C_D_ID;
    private Integer H_C_W_ID;
    private Integer H_D_ID;
    private Integer H_W_ID;


    public History( Integer h_C_ID, Integer h_C_D_ID, Integer h_C_W_ID, Timestamp h_DATE,
            Double h_AMOUNT,
            String h_DATA, Integer h_d_id, Integer h_w_id ) {
        H_C_ID = h_C_ID;
        H_C_D_ID = h_C_D_ID;
        H_C_W_ID = h_C_W_ID;
        H_DATE = h_DATE;
        H_AMOUNT = h_AMOUNT;
        H_DATA = h_DATA;
        H_D_ID = h_d_id;
        H_W_ID = h_w_id;
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public History( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public History() {
    }


    public Integer getH_D_ID() {
        return H_D_ID;
    }


    public void setH_D_ID( Integer h_D_ID ) {
        H_D_ID = h_D_ID;
    }


    public Integer getH_W_ID() {
        return H_W_ID;
    }


    public void setH_W_ID( Integer h_W_ID ) {
        H_W_ID = h_W_ID;
    }


    public Integer getH_C_ID() {
        return H_C_ID;
    }


    public void setH_C_ID( Integer h_C_ID ) {
        H_C_ID = h_C_ID;
    }


    public Integer getH_C_D_ID() {
        return H_C_D_ID;
    }


    public void setH_C_D_ID( Integer h_C_D_ID ) {
        H_C_D_ID = h_C_D_ID;
    }


    public Integer getH_C_W_ID() {
        return H_C_W_ID;
    }


    public void setH_C_W_ID( Integer h_C_W_ID ) {
        H_C_W_ID = h_C_W_ID;
    }


    public Timestamp getH_DATE() {
        return H_DATE;
    }


    public void setH_DATE( Timestamp h_DATE ) {
        H_DATE = h_DATE;
    }


    public Double getH_AMOUNT() {
        return H_AMOUNT;
    }


    public void setH_AMOUNT( Double h_AMOUNT ) {
        H_AMOUNT = h_AMOUNT;
    }


    public String getH_DATA() {
        return H_DATA;
    }


    public void setH_DATA( String h_DATA ) {
        H_DATA = h_DATA;
    }


    @Override
    public String toString() {
        return "History{" +
                "H_DATE=" + H_DATE +
                ", H_AMOUNT=" + H_AMOUNT +
                ", H_DATA='" + H_DATA + '\'' +
                ", H_C_ID=" + H_C_ID +
                ", H_C_D_ID=" + H_C_D_ID +
                ", H_C_W_ID=" + H_C_W_ID +
                '}';
    }
}
