package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.objects;


import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.checkStringLength;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.generateDistrict;
import static ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest.verifyZip;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.TPCCPopulationGenerator;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.Customer;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.objects.District;
import ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc.population.TPCCPopulationGeneratorTest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * @author Silvan Heller
 * @tpccversion 5.11
 *
 * Created by silvan on 23.04.17.
 */
public class CustomerTest {

    private static final Logger logger = LogManager.getLogger();


    @Test
    public void generateCustomersForDistrict() throws Exception {
        District district = generateDistrict();
        final Timestamp C_SINCE = Timestamp.from( Instant.now() );
        Customer[] customers = TPCCPopulationGenerator
                .generateCustomersForDistrict( district, C_SINCE );
        HashSet<Integer> ids = new HashSet<>();
        int BC = 0;
        for ( Customer customer : customers ) {
            ids.add( customer.getC_ID() );
            if ( !customer.getC_CREDIT().equals( "GC" ) ) {
                if ( !customer.getC_CREDIT().equals( "BC" ) ) {
                    logger.error( "C_CREDIT was {}", customer.getC_CREDIT() );
                    fail();
                }
                BC++;
            }
            verifyCustomer( customer, district, C_SINCE );
        }
        if ( ids.size() != 3000 ) {
            logger.error( "{} unique ids were generated", ids.size() );
            fail();
        }
        if ( BC == customers.length / 10 ) {
            logger.warn( "BC was in precisely 10% of customers" );
        }
        if ( Math.abs( BC - customers.length / 10 ) > customers.length / 100 * 5 ) {
            logger.error( "BC was in {} of {} customers", BC, customers.length );
            fail();
        }
    }


    /**
     * Checks if a given {@link Customer} is valid and belongs to the given {@link District}. C_CREDIT
     * Verification is not done, it is expected to be done outside this method since it involves
     * counting over multiple customers.
     *
     * @param C_SINCE Timestamp which was passed to the customer generation procedure.
     */
    private void verifyCustomer( Customer customer, District district, final Timestamp C_SINCE ) {
        if ( customer.getC_D_ID() != district.getD_ID() ) {
            fail();
        }
        if ( customer.getC_W_ID() != district.getD_W_ID() ) {
            fail();
        }
        TPCCPopulationGeneratorTest.verifyCLAST( customer.getC_LAST() );
        if ( !customer.getC_MIDDLE().equals( "OE" ) ) {
            logger.error( "C_MIDDLE was {}", customer.getC_MIDDLE() );
            fail();
        }
        checkStringLength( customer.getC_FIRST(), 8, 16 );
        checkStringLength( customer.getC_STREET_1(), 10, 20 );
        checkStringLength( customer.getC_STREET_2(), 10, 20 );
        checkStringLength( customer.getC_CITY(), 10, 20 );
        checkStringLength( customer.getC_STATE(), 2, 2 );
        verifyZip( customer.getC_ZIP() );
        if ( customer.getC_PHONE().length() != 16 ) {
            logger.error( "C_PHONE was {}", customer.getC_PHONE() );
            fail();
        }
        //See if C_PHONE is numeric
        try {
            Integer.parseInt( customer.getC_PHONE().substring( 0, 8 ) );
            Integer.parseInt( customer.getC_PHONE().substring( 8, 16 ) );
        } catch ( NumberFormatException e ) {
            logger.error( "C_PHONE was {}", customer.getC_PHONE() );
            fail();
        }
        if ( !customer.getC_SINCE().equals( C_SINCE ) ) {
            fail();
        }
        if ( customer.getC_CREDIT_LIM() != 50000 ) {
            logger.error( "C_CREDIT_LIM was {}", customer.getC_CREDIT_LIM() );
            fail();
        }
        if ( customer.getC_BALANCE() != -10 ) {
            logger.error( "C_BALANCE was {}", customer.getC_BALANCE() );
            fail();
        }
        if ( customer.getC_YTD_PAYMENT() != 10 ) {
            logger.error( "C_YTD_PAYMENT was {}", customer.getC_YTD_PAYMENT() );
            fail();
        }
        if ( customer.getC_PAYMENT_CNT() != 1 ) {
            logger.error( "C_PAYMENT_CNT was {}", customer.getC_PAYMENT_CNT() );
            fail();
        }
        if ( customer.getC_DELIVERY_CNT() != 0 ) {
            logger.error( "C_DELIVERY_CNT was {}", customer.getC_DELIVERY_CNT() );
            fail();
        }
        checkStringLength( customer.getC_DATA(), 300, 500 );
    }
}
