package org.polypheny.client.db.access;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import org.apache.http.HttpEntity;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * RESTConnector tailored for Icarus
 *
 * @author silvan on 17.07.17.
 */
public class RESTConnector implements DBConnector {

    private static final Logger logger = LogManager.getLogger();
    private CloseableHttpClient httpclient = HttpClients.createDefault();
    private HttpPost httpPost;
    private ResponseHandler<String> responseHandler;
    private String url;
    private int port;
    private JsonObject query = new JsonObject();
    private String icarusURL;


    public RESTConnector( String url, int port ) {
        Unirest.setTimeouts( 0, 0 );
        Unirest.setConcurrency( 200, 100 );
        this.url = url;
        this.port = port;
        query.addProperty( "queryClass", "1" );
        query.addProperty( "resultMode", "DEBUG" );
        query.addProperty( "targetExecutionTime", "500" );
        icarusURL = "http://" + url + ":" + port + "/request";
        httpPost = new HttpPost( icarusURL );
        responseHandler = response -> {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString( entity ) : null;
        };

    }


    /**
     * Execute POST-Request via http-client library
     */
    private synchronized String postHTTPClient( String sql ) {
        query.addProperty( "sql", sql );
        StringEntity input = null;
        try {
            input = new StringEntity( query.toString() );
            input.setContentType( "application/json" );
            httpPost.setEntity( input );
            return httpclient.execute( httpPost, responseHandler );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * Execute a POST-Request with the same code as in icarus-client
     */
    private synchronized String postIcarusClient( String sql ) {
        HttpResponse<InputStream> httpResponse;
        try {
            query.addProperty( "sql", sql );
            httpResponse = Unirest.post( icarusURL ).body( query.toString() ).asBinary();
            BufferedReader rd = new BufferedReader( new InputStreamReader( httpResponse.getRawBody() ) );
            StringBuilder response = new StringBuilder();
            String line;
            while ( (line = rd.readLine()) != null ) {
                response.append( line );
                response.append( '\n' );
            }
            rd.close();
            return response.toString();
        } catch ( UnirestException | IOException e ) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * Performs a simple post request to the given url, returning the string.
     *
     * @param sql sql-text
     */
    private synchronized String postRequest( String sql ) throws ConnectionException {
        try {
            query.addProperty( "sql", sql );
            String request = Unirest.post( "http://" + url + ":" + port + "/request" ).body( query.toString() ).asString().getBody();
            JsonObject obj = new JsonParser().parse( request ).getAsJsonObject();
            if ( obj.getAsJsonPrimitive( "responseCode" ).getAsInt() != 200 ) {
                if ( sql.length() < 1000 ) {
                    logger.debug( "Error for queryString {}", sql );
                } else {
                    logger.trace( "Error for queryString {}", sql );
                }
                logger.debug( "Error message: {}", obj.getAsJsonPrimitive( "errorMessage" ).getAsString() );
                throw new ConnectionException( obj.toString() );
            }
            return request;
        } catch ( UnirestException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void startTransaction() {
        logger.trace( "REST does not support transactions ATM" );
    }


    @Override
    public void commitTransaction() {
        logger.trace( "REST does not support transactions ATM" );
    }


    @Override
    public void abortTransaction() {
        logger.trace( "REST does nto support transactions ATM" );
    }


    @Override
    public ResultSet executeQuery( String query ) throws ConnectionException {
        String result = postRequest( query );
        return new IcarusResultSet( result );
    }


    @Override
    public void executeStatement( String statement ) throws ConnectionException {
        logger.trace( postRequest( statement ) );
    }


    @Override
    public void executeScript( File file ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        logger.trace( "The REST-Connector does not bind any resources" );
    }
}
