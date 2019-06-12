package ch.unibas.dmi.dbis.polyphenydb.client.generator.tpcc;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author silvan on 19.07.17.
 */
public class Utils {

    private static final Logger logger = LogManager.getLogger();


    public static void fillClassViaReflection( Object object, ResultSet resultSet ) {
        for ( Method method : object.getClass().getMethods() ) {
            if ( method.getName().contains( "set" ) ) {
                Object inputValue = null;
                Class parameterType = null;
                try {
                    try {
                        inputValue = resultSet.getObject( method.getName().replace( "set", "" ) );
                    } catch ( SQLException e ) {
                        logger.trace( e );
                    }
                    if ( inputValue == null ) {
                        inputValue = resultSet.getObject( method.getName().replace( "set", "" ).toLowerCase() );
                        if ( inputValue == null ) {
                            method.invoke( object, inputValue );
                            continue;
                        }
                    }
                    parameterType = method.getParameterTypes()[0];
                    if ( parameterType.equals( Integer.class ) ) {
                        inputValue = (int) Float.parseFloat( inputValue.toString().trim() );
                    }
                    if ( parameterType.equals( Double.class ) || parameterType.equals( double.class ) ) {
                        inputValue = Double.parseDouble( inputValue.toString() );
                    }
                    if ( inputValue.getClass().equals( BigDecimal.class ) ) {
                        inputValue = ((BigDecimal) inputValue).doubleValue();
                    }
                    if ( parameterType.equals( Timestamp.class ) ) {
                        try {
                            inputValue = Timestamp.valueOf( inputValue.toString() );
                        } catch ( IllegalArgumentException e ) {
                            try {
                                SimpleDateFormat dateFormat = new SimpleDateFormat( "MMM dd, yyyy h:mm:ss a" );
                                Date parsedDate = dateFormat.parse( inputValue.toString() );
                                inputValue = new Timestamp( parsedDate.getTime() );
                            } catch ( Exception general ) {
                                general.printStackTrace();
                            }
                        }
                    }
                    method.invoke( object, inputValue );
                } catch ( IllegalAccessException | InvocationTargetException | IllegalArgumentException e ) {
                    logger.error( "Error invoking method {} with argument {}", method.getName(), inputValue );
                    try {
                        logger.error( "Original Argument: {}", resultSet.getObject( method.getName().replace( "set", "" ) ) );
                    } catch ( SQLException e1 ) {
                        logger.trace( e1 );
                    }
                    logger.error( "Class of param: {} and class of input {}", parameterType, inputValue.getClass() );
                    logger.error( e );
                } catch ( SQLException | UnsupportedOperationException e ) {
                    logger.trace( e );
                }
            }
        }
    }

}
