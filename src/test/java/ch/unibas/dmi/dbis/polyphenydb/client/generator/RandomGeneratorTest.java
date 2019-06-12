package ch.unibas.dmi.dbis.polyphenydb.client.generator;


import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


/**
 * Tests our Random-Generator
 *
 * Created by silvan on 11.04.17.
 */
public class RandomGeneratorTest {

    private Logger logger = LogManager.getLogger();


    @Test
    public void testUniformBounds() {
        boolean lbSeen = false;
        boolean ubSeen = false;
        for ( int i = 0; i < 1000; i++ ) {
            int gen = RandomGenerator.generateUniform( 1, 2 );
            if ( gen == 1 ) {
                lbSeen = true;
            }
            if ( gen == 2 ) {
                ubSeen = true;
            }
        }
        if ( !lbSeen || !ubSeen ) {
            logger.error( "Seen 1: {}, Seen 2: {}", lbSeen, ubSeen );
            fail();
        }
    }


    @Test
    public void testUniform() {
        Random rand = new Random();
        for ( int i = 0; i < 10000; i++ ) {
            int one = rand.nextInt();
            int two = rand.nextInt();

            int uniform = RandomGenerator.generateUniform( Math.min( one, two ), Math.max( one, two ) );
            if ( uniform < Math.min( one, two ) || uniform > Math.max( one, two ) ) {
                logger.error( "Random numbers {} and {} generated uniform {} ", one, two, uniform );
                fail();
            }
        }
    }


    @Test
    public void testRandomAString() {
        HashSet<Character> set = new HashSet<>();
        for ( int i = 0; i < 10000; i++ ) {
            String rand = RandomGenerator.generateAString( 50, 100 );
            if ( rand.length() < 50 || rand.length() > 100 ) {
                logger.error( "String length was invalid. Generated String is {}", rand );
                fail();
            }
            for ( Character c : rand.toCharArray() ) {
                set.add( c );
            }
        }
        if ( set.size() < 128 ) {
            logger.error( "The generated strings contain less than 128 unique characters, namely only {}",
                    set.size() );
            fail();
        }
    }


    @Test
    public void testRandomNString() {
        HashSet<Integer> set = new HashSet<>();
        for ( int i = 0; i < 10000; i++ ) {
            String rand = RandomGenerator.generateNString( 50, 100 );
            if ( rand.length() < 50 || rand.length() > 100 ) {
                logger.error( "String length was invalid. Generated String is {}", rand );
                fail();
            }
            for ( Character c : rand.toCharArray() ) {
                set.add( Character.getNumericValue( c ) );
            }
        }
        if ( set.size() != 10 ) {
            logger.error(
                    "The generated strings do not contain exactly 10 unique integers. Instead, they contain {}",
                    set.size() );
            fail();
        }
    }
}
