package ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.icarus;


import ch.unibas.dmi.dbis.polyphenydb.client.db.access.IcarusResultSet;
import ch.unibas.dmi.dbis.polyphenydb.client.db.access.RESTConnector;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.IcarusTpccBenchmarker;
import ch.unibas.dmi.dbis.polyphenydb.client.db.tpcc.transactions.PaymentTransactionExecutor;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.District;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Warehouse;
import ch.unibas.dmi.dbis.polyphenydb.client.grpc.PolyClientGRPC.QueryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author silvan on 18.07.17.
 */
public class IcarusPaymentTransactionExecutor extends PaymentTransactionExecutor {

    private static final Logger logger = LogManager.getLogger();
    private RESTConnector connector;
    private IcarusTpccBenchmarker benchmarker;


    public IcarusPaymentTransactionExecutor( RESTConnector connector, IcarusTpccBenchmarker benchmarker ) {
        super( connector, benchmarker );
        this.connector = connector;
        this.benchmarker = benchmarker;
    }


    @Override
    protected Customer handleBCforCustomer( Customer customer, int d_id, int w_id, Double h_amount ) {
        String query = "SELECT C_DATA FROM tpcc_customer WHERE C_W_ID=" + customer.getC_W_ID() + " AND C_D_ID=" + customer.getC_D_ID() + " AND C_ID=" + customer.getC_ID();

        return executeQuery( queryResultSet -> {
            String C_DATA = queryResultSet.getString( "C_DATA" );
            String historyInfo = "" + customer.getC_ID() + customer.getC_D_ID() + customer.getC_W_ID() + d_id + w_id + h_amount;
            int oldlen = C_DATA.length();
            C_DATA = C_DATA.substring( 0, C_DATA.length() - historyInfo.length() ) + historyInfo;
            customer.setC_DATA( C_DATA );
            int newlen = C_DATA.length();
            if ( oldlen != newlen ) {
                logger.error( "old length {}, new length {}", oldlen, newlen );
            }
            String update = "UPDATE tpcc_customer SET C_DATA='" + C_DATA + "' WHERE C_W_ID=" + customer.getC_W_ID() + " AND C_D_ID=" + customer.getC_D_ID() + " AND C_ID=" + customer.getC_ID();
            executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );
            return customer;
        }, query, QueryType.QUERYTYPESELECT );
    }


    @Override
    protected Customer getCustomerAndPay( int c_w_id, int c_d_id, Integer c_id, Double h_amount ) {
        String update = "update tpcc_customer SET C_BALANCE=c_balance-" + h_amount + ", c_ytd_payment=c_ytd_payment+" + h_amount + ",c_payment_cnt=c_payment_cnt+1 where c_id=" + c_id + " and c_d_id=" + c_d_id + " and c_w_id=" + c_w_id;
        executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );

        String query = "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE FROM tpcc_customer where c_id=" + c_id + " and c_d_id=" + c_d_id + " and c_w_id=" + c_w_id;
        return executeQuery( Customer::new, query, QueryType.QUERYTYPESELECT );
    }


    @Override
    protected Customer getCustomerAndPay( int c_w_id, int c_d_id, String C_LAST, Double h_amount ) {
        String query = "SELECT * FROM tpcc_customer WHERE C_W_ID=" + c_w_id + " AND C_D_ID=" + c_d_id + " AND C_LAST= '" + C_LAST + "' ORDER BY C_FIRST ASC";
        Customer customer = executeQuery( resultSet -> {
            IcarusResultSet rs = (IcarusResultSet) resultSet;
            int index;
            int count = rs.size();
            if ( count == 0 ) {
                logger.error( "No elements retrieved for query {}. Aborting.", query );
                throw new RuntimeException();
            }
            if ( count % 2 == 0 ) {
                index = count / 2;
            } else {
                index = count / 2 + 1;
            }
            int counter = 1; //remember, we are already in the first row
            while ( counter < index ) {
                counter++;
                resultSet.next();
            }
            return new Customer( resultSet );
        }, query, QueryType.QUERYTYPESELECT );

        String update = "UPDATE tpcc_customer SET c_balance = c_balance - " + h_amount + ", c_ytd_payment = c_ytd_payment + " + h_amount + ", c_payment_cnt = c_payment_cnt + 1 WHERE C_W_ID = " + c_w_id + " AND C_D_ID=" + c_d_id + " AND C_ID=" + customer.getC_ID();
        executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );
        return customer;
    }


    @Override
    protected District getDistrictAndIncYTD( int D_W_ID, int D_ID, Double h_amount ) {
        String update = "update tpcc_district set d_ytd=d_ytd+" + h_amount + " WHERE d_w_id=" + D_W_ID + " and d_id=" + D_ID;
        executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );

        String query = "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM tpcc_district WHERE d_w_id=" + D_W_ID + " and D_ID=" + D_ID;
        return executeQuery( District::new, query, QueryType.QUERYTYPESELECT );
    }


    @Override
    protected Warehouse getWarehouseAndIncYTD( int W_ID, Double H_AMOUNT ) {
        String update = "UPDATE tpcc_warehouse SET w_ytd=w_ytd+" + H_AMOUNT + " WHERE w_id=" + W_ID;
        executeAndLogStatement( update, QueryType.QUERYTYPEUPDATE );

        String query = "SELECT w_name, w_street_1,  w_street_2, w_city, w_state, w_zip FROM tpcc_warehouse WHERE w_id=" + W_ID;
        return executeQuery( Warehouse::new, query, QueryType.QUERYTYPESELECT );
    }

}
