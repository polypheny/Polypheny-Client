package org.polypheny.client.generator.tpcc.population;


import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.polypheny.client.generator.tpcc.TPCCGenerator;
import org.polypheny.client.generator.tpcc.TPCCPopulationGenerator;
import org.polypheny.client.generator.tpcc.objects.Customer;
import org.polypheny.client.generator.tpcc.objects.District;
import org.polypheny.client.generator.tpcc.objects.Order;
import org.polypheny.client.generator.tpcc.objects.Warehouse;


/**
 * {@link TPCCPopulationGenerator}
 */
public class TPCCPopulationGeneratorTest {

    private static final Logger logger = LogManager.getLogger();


    public static Warehouse generateWarehouse() {
        int W_ID = 1;
        return TPCCPopulationGenerator.generateWarehouse( W_ID );
    }


    public static District generateDistrict() {
        Warehouse warehouse = generateWarehouse();
        int D_ID = 1;
        return TPCCPopulationGenerator.generateDistrict( warehouse, D_ID );
    }


    public static Customer generateCustomer() {
        District district = generateDistrict();
        int C_ID = 1;
        return TPCCPopulationGenerator
                .generateCustomer( district, C_ID, TPCCPopulationGenerator
                                .generateC_LAST( TPCCGenerator.getCLast( TPCCGenerator.getcLoad() ) ),
                        Timestamp.from( Instant.now() ) );
    }


    public static Order generateOrder() {
        District district = generateDistrict();
        int C_ID = 1;
        int O_ID = 1;
        return TPCCPopulationGenerator.generateOrder( C_ID, district, O_ID );
    }


    public static void checkStringLength( String string, int lb, int ub ) {
        if ( string.length() < lb || string.length() > ub ) {
            logger
                    .error( "String length was {}, which is not within {} and {}", string.length(), lb, ub );
            fail();
        }
    }


    @Test
    public void generateZIP() {
        HashSet<Integer> uniqueZIPs = new HashSet<>();
        for ( int i = 0; i < 100000; i++ ) {
            int zip = TPCCPopulationGenerator.generateZIP();
            verifyZip( zip );
            uniqueZIPs.add( zip );
        }
        if ( uniqueZIPs.size() > 10000 ) {
            logger.error( "{} unique zips were generated", uniqueZIPs.size() );
            fail();
        }
        if ( uniqueZIPs.size() != 10000 ) {
            logger.warn( "{} only unique zips were generated", uniqueZIPs.size() );
        }
    }


    /**
     * Verifies a zip according to 4.3.2.7
     */
    public static void verifyZip( int zip ) {
        String zipString = Integer.toString( zip );
        String suffix = zipString.substring( zipString.length() - 5 );
        String prefix = zipString.substring( 0, zipString.length() - 5 );
        if ( prefix.equals( "" ) ) {
            prefix = "0"; //Fixes 0s being empty strings
        }
        if ( Integer.parseInt( prefix ) > 9999 ) {
            logger.error( "prefix {} for zip {}", prefix, zip );
            fail();
        }
        if ( !suffix.equals( "11111" ) ) {
            logger.error( "zip {}, zipString {} and suffix {}", zip, zipString, suffix );
            fail();
        }
    }


    @Test
    public void generateCLast() {
        for ( int ones = 0; ones < 9; ones++ ) {
            for ( int tens = 0; tens < 9; tens++ ) {
                for ( int hundreds = 0; hundreds < 9; hundreds++ ) {
                    String rand = TPCCPopulationGenerator.generateC_LAST( hundreds * 100 + tens * 10 + ones );
                    if ( !rand.startsWith( TPCCPopulationGenerator.C_LAST_SYLLABLES[hundreds] ) ) {
                        logger.error( "C_LAST: {}, number: {}, Syllable: {} ", rand,
                                hundreds * 100 + tens * 10 + ones,
                                TPCCPopulationGenerator.C_LAST_SYLLABLES[hundreds] );
                        logger.debug( hundreds + "" + tens + "" + ones );
                        fail();
                    }
                    if ( !rand.contains( TPCCPopulationGenerator.C_LAST_SYLLABLES[tens] ) ) {
                        fail();
                    }
                    if ( !rand.endsWith( TPCCPopulationGenerator.C_LAST_SYLLABLES[ones] ) ) {
                        fail();
                    }
                    String s = checkStart( rand );
                    checkMiddle( rand, s );
                    checkEnd( rand );
                }
            }
        }
    }


    /**
     * Verifies if a C_LAST is valid given the generated number.
     *
     * @param number the number used to generate the syllables of C_LAST
     * @param C_LAST the generated last name of the customer. Should be generated using {@link
     * TPCCPopulationGenerator#generateC_LAST(int)}
     */
    public static void verifyCLAST( int number, String C_LAST ) {
        if ( !C_LAST.startsWith(
                TPCCPopulationGenerator.C_LAST_SYLLABLES[number - number % 100 - number % 10] ) ) {
            fail();
        }
        if ( !C_LAST
                .contains( TPCCPopulationGenerator.C_LAST_SYLLABLES[number % 100 - number % 10] ) ) {
            fail();
        }
        if ( !C_LAST.endsWith( TPCCPopulationGenerator.C_LAST_SYLLABLES[number % 10] ) ) {
            fail();
        }
        verifyCLAST( C_LAST );
    }


    /**
     * Verifies if a given customer name only contains valid syllables
     *
     * @param C_LAST generated customer last name using {@link TPCCPopulationGenerator#generateC_LAST(int)}
     */
    public static void verifyCLAST( String C_LAST ) {
        String s = checkStart( C_LAST );
        checkMiddle( C_LAST, s );
        checkEnd( C_LAST );
    }


    private static void checkMiddle( String rand, String startSyllable ) {
        for ( String s : TPCCPopulationGenerator.C_LAST_SYLLABLES ) {
            if ( rand.substring( startSyllable.length() ).startsWith( s ) ) {
                return;
            }
        }
        logger.error(
                "Generated String does have an allowed syllable in the middle. We expected the string to start with {}. The generated string was {}",
                startSyllable, rand );
        fail();
    }


    /**
     * Checks if the generated string ends with an allowed syllable
     */
    private static void checkEnd( String rand ) {
        for ( String s : TPCCPopulationGenerator.C_LAST_SYLLABLES ) {
            if ( rand.endsWith( s ) ) {
                return;
            }
        }
        logger.error( "Generated String does not end with a specified syllable. Instead, it was {}",
                rand );
        fail();
    }


    /**
     * Checks if the generated string starts with an allowed syllable
     *
     * @return the syllable the random string started with
     */
    private static String checkStart( String rand ) {
        for ( String s : TPCCPopulationGenerator.C_LAST_SYLLABLES ) {
            if ( rand.startsWith( s ) ) {
                return s;
            }
        }
        logger.error( "Generated String does not start with a specified syllable. Instead, it was {}",
                rand );
        fail();
        return null;  //Not reached since we fail
    }

}