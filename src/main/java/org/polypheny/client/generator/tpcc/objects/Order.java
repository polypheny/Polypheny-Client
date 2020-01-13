package org.polypheny.client.generator.tpcc.objects;


import java.sql.ResultSet;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.generator.tpcc.Utils;


/**
 * An Order Item According to the TPC-C Specifications 5.11
 *
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class Order {

    private static final Logger logger = LogManager.getLogger();
    private Integer O_ID;
    private Integer O_C_ID;
    private Integer O_D_ID;
    private Integer O_W_ID;
    private Timestamp O_ENTRY_D;
    private Integer O_CARRIER_ID;
    private Integer O_OL_CNT;
    private Integer O_ALL_LOCAL;


    public Order() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public Order( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public Order( Integer o_ID, Integer o_C_ID, Integer o_D_ID, Integer o_W_ID, Timestamp o_ENTRY_D,
            Integer o_CARRIER_ID,
            Integer o_OL_CNT, Integer o_ALL_LOCAL ) {
        O_ID = o_ID;
        O_C_ID = o_C_ID;
        O_D_ID = o_D_ID;
        O_W_ID = o_W_ID;
        O_ENTRY_D = o_ENTRY_D;
        O_CARRIER_ID = o_CARRIER_ID;
        O_OL_CNT = o_OL_CNT;
        O_ALL_LOCAL = o_ALL_LOCAL;
    }


    public Integer getO_ID() {
        return O_ID;
    }


    public void setO_ID( Integer o_ID ) {
        O_ID = o_ID;
    }


    public Integer getO_C_ID() {
        return O_C_ID;
    }


    public void setO_C_ID( Integer o_C_ID ) {
        O_C_ID = o_C_ID;
    }


    public Integer getO_D_ID() {
        return O_D_ID;
    }


    public void setO_D_ID( Integer o_D_ID ) {
        O_D_ID = o_D_ID;
    }


    public Integer getO_W_ID() {
        return O_W_ID;
    }


    public void setO_W_ID( Integer o_W_ID ) {
        O_W_ID = o_W_ID;
    }


    public Timestamp getO_ENTRY_D() {
        return O_ENTRY_D;
    }


    public void setO_ENTRY_D( Timestamp o_ENTRY_D ) {
        O_ENTRY_D = o_ENTRY_D;
    }


    @Override
    public String toString() {
        return "Order{" +
                "O_ID=" + O_ID +
                ", O_C_ID=" + O_C_ID +
                ", O_D_ID=" + O_D_ID +
                ", O_W_ID=" + O_W_ID +
                ", O_ENTRY_D=" + O_ENTRY_D +
                ", O_CARRIER_ID=" + O_CARRIER_ID +
                ", O_OL_CNT=" + O_OL_CNT +
                ", O_ALL_LOCAL=" + O_ALL_LOCAL +
                '}';
    }


    @Nullable
    public Integer getO_CARRIER_ID() {
        return O_CARRIER_ID;
    }


    public void setO_CARRIER_ID( Integer o_CARRIER_ID ) {
        O_CARRIER_ID = o_CARRIER_ID;
    }


    public Integer getO_OL_CNT() {
        return O_OL_CNT;
    }


    public void setO_OL_CNT( Integer o_OL_CNT ) {
        O_OL_CNT = o_OL_CNT;
    }


    public Integer getO_ALL_LOCAL() {
        return O_ALL_LOCAL;
    }


    public void setO_ALL_LOCAL( Integer o_ALL_LOCAL ) {
        O_ALL_LOCAL = o_ALL_LOCAL;
    }
}
