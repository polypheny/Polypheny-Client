package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects;


import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.Utils;
import java.sql.ResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A row with the contents of a TPC-C Benchmark Item as specified in 4.3.3.1 <p> Created by silvan on 20.04.17.
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class Item {

    private static final Logger logger = LogManager.getLogger();
    private Integer I_ID;
    /**
     * Image ID associated with the item
     */
    private Integer I_IM_ID;
    private String I_NAME;
    private Double I_PRICE;
    private String I_DATA;


    public Item() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public Item( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public Item( Integer i_id, Integer i_im_id, String i_name, Double i_price, String i_data ) {
        I_ID = i_id;
        I_IM_ID = i_im_id;
        I_NAME = i_name;
        I_PRICE = i_price;
        I_DATA = i_data;
    }


    public Integer getI_ID() {
        return I_ID;
    }


    public void setI_ID( Integer i_ID ) {
        I_ID = i_ID;
    }


    public Integer getI_IM_ID() {
        return I_IM_ID;
    }


    public void setI_IM_ID( Integer i_IM_ID ) {
        I_IM_ID = i_IM_ID;
    }


    public String getI_NAME() {
        return I_NAME;
    }


    public void setI_NAME( String i_NAME ) {
        I_NAME = i_NAME;
    }


    public Double getI_PRICE() {
        return I_PRICE;
    }


    public void setI_PRICE( Double i_PRICE ) {
        I_PRICE = i_PRICE;
    }


    public String getI_DATA() {
        return I_DATA;
    }


    public void setI_DATA( String i_DATA ) {
        I_DATA = i_DATA;
    }


    @Override
    public String toString() {
        return "Item{" +
                "I_ID=" + I_ID +
                ", I_IM_ID=" + I_IM_ID +
                ", I_NAME='" + I_NAME + '\'' +
                ", I_PRICE=" + I_PRICE +
                ", I_DATA='" + I_DATA + '\'' +
                '}';
    }
}
