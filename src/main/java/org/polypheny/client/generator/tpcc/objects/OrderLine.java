package org.polypheny.client.generator.tpcc.objects;


import java.sql.ResultSet;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.polypheny.client.generator.tpcc.Utils;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 */
public class OrderLine {

    private static final Logger logger = LogManager.getLogger();
    private Integer OL_O_ID;
    private Integer OL_D_ID;
    private Integer OL_W_ID;
    private Integer OL_NUMBER;
    private Integer OL_I_ID;
    private Integer OL_SUPPLY_W_ID;
    private Timestamp OL_DELIVERY_D;
    private Integer OL_QUANTITY;
    private Double OL_AMOUNT;
    private String OL_DIST_INFO;


    public OrderLine() {
    }


    /**
     * Fills this object via reflection on getters / setters. This requires that the name of the setters is exactly setNAME, where NAME is named the same way in the ResultSet.
     */
    public OrderLine( ResultSet resultSet ) {
        Utils.fillClassViaReflection( this, resultSet );
    }


    public OrderLine( Integer OL_O_ID, Integer OL_D_ID, Integer OL_W_ID, Integer OL_NUMBER,
            Integer OL_I_ID,
            Integer OL_SUPPLY_W_ID, Timestamp OL_DELIVERY_D, Integer OL_QUANTITY, Double OL_AMOUNT,
            String OL_DIST_INFO ) {
        this.OL_O_ID = OL_O_ID;
        this.OL_D_ID = OL_D_ID;
        this.OL_W_ID = OL_W_ID;
        this.OL_NUMBER = OL_NUMBER;
        this.OL_I_ID = OL_I_ID;
        this.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
        this.OL_DELIVERY_D = OL_DELIVERY_D;
        this.OL_QUANTITY = OL_QUANTITY;
        this.OL_AMOUNT = OL_AMOUNT;
        this.OL_DIST_INFO = OL_DIST_INFO;
    }


    public Integer getOL_O_ID() {
        return OL_O_ID;
    }


    public void setOL_O_ID( Integer OL_O_ID ) {
        this.OL_O_ID = OL_O_ID;
    }


    public Integer getOL_D_ID() {
        return OL_D_ID;
    }


    public void setOL_D_ID( Integer OL_D_ID ) {
        this.OL_D_ID = OL_D_ID;
    }


    public Integer getOL_W_ID() {
        return OL_W_ID;
    }


    public void setOL_W_ID( Integer OL_W_ID ) {
        this.OL_W_ID = OL_W_ID;
    }


    public Integer getOL_NUMBER() {
        return OL_NUMBER;
    }


    public void setOL_NUMBER( Integer OL_NUMBER ) {
        this.OL_NUMBER = OL_NUMBER;
    }


    public Integer getOL_I_ID() {
        return OL_I_ID;
    }


    public void setOL_I_ID( Integer OL_I_ID ) {
        this.OL_I_ID = OL_I_ID;
    }


    public Integer getOL_SUPPLY_W_ID() {
        return OL_SUPPLY_W_ID;
    }


    public void setOL_SUPPLY_W_ID( Integer OL_SUPPLY_W_ID ) {
        this.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
    }


    @Nullable
    public Timestamp getOL_DELIVERY_D() {
        return OL_DELIVERY_D;
    }


    public void setOL_DELIVERY_D( Timestamp OL_DELIVERY_D ) {
        this.OL_DELIVERY_D = OL_DELIVERY_D;
    }


    public Integer getOL_QUANTITY() {
        return OL_QUANTITY;
    }


    public void setOL_QUANTITY( Integer OL_QUANTITY ) {
        this.OL_QUANTITY = OL_QUANTITY;
    }


    public Double getOL_AMOUNT() {
        return OL_AMOUNT;
    }


    public void setOL_AMOUNT( Double OL_AMOUNT ) {
        this.OL_AMOUNT = OL_AMOUNT;
    }


    public String getOL_DIST_INFO() {
        return OL_DIST_INFO;
    }


    public void setOL_DIST_INFO( String OL_DIST_INFO ) {
        this.OL_DIST_INFO = OL_DIST_INFO;
    }


    @Override
    public String toString() {
        return "OrderLine{" +
                "OL_O_ID=" + OL_O_ID +
                ", OL_D_ID=" + OL_D_ID +
                ", OL_W_ID=" + OL_W_ID +
                ", OL_NUMBER=" + OL_NUMBER +
                ", OL_I_ID=" + OL_I_ID +
                ", OL_SUPPLY_W_ID=" + OL_SUPPLY_W_ID +
                ", OL_DELIVERY_D=" + OL_DELIVERY_D +
                ", OL_QUANTITY=" + OL_QUANTITY +
                ", OL_AMOUNT=" + OL_AMOUNT +
                ", OL_DIST_INFO='" + OL_DIST_INFO + '\'' +
                '}';
    }
}
