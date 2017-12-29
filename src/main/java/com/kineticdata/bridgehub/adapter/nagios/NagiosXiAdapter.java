package com.kineticdata.bridgehub.adapter.nagios;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.DocumentContext;
import com.kineticdata.bridgehub.helpers.http.HttpGetWithEntity;
import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import com.jayway.jsonpath.JsonPath;
import java.net.URLEncoder;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;

public class NagiosXiAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/

    /** Defines the adapter display name */
    public static final String NAME = "Nagios XI Bridge";
    public static final String JSON_ROOT_DEFAULT = "$.";

    /** Defines the LOGGER */
    protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NagiosXiAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(NagiosXiAdapter.class.getResourceAsStream("/"+NagiosXiAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            LOGGER.warn("Unable to load "+NagiosXiAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    private String apiKey;
    private String apiEndpoint;

    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String API_KEY = "API Key";
        public static final String API_URL = "Nagios XI URL";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.API_KEY),
        new ConfigurableProperty(Properties.API_URL)
    );


    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public void initialize() throws BridgeError {
        this.apiKey = properties.getValue(Properties.API_KEY);
        // Remove any trailing forward slash.
        this.apiEndpoint = properties.getValue(Properties.API_URL).replaceFirst("(\\/)$", "");
        testAuthenticationValues(this.apiEndpoint, this.apiKey);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {

        NagiosQualificationParser nagiosParser = new NagiosQualificationParser();
        String jsonResponse = nagiosQuery("count", request, nagiosParser);
        Long count = JsonPath.parse(jsonResponse).read("$[*].recordcount", Long.class);

        // Create and return a Count object.
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {

        NagiosQualificationParser nagiosParser = new NagiosQualificationParser();
        String jsonRootPath = NagiosQualificationParser.jsonPathMapping.get(request.getStructure());
       
        String jsonResponse = nagiosQuery("search", request, nagiosParser);

        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Record recordResult = new Record(null);

        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            if (listRoot.size() == 1) {
                Map<String, Object> recordValues = new HashMap();
                request.getFields().forEach((field) -> {
                    try {
                        recordValues.put(field, JsonPath.parse(listRoot.get(0)).read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                });
                recordResult = new Record(recordValues);
            } else {
                throw new BridgeError("Multiple results matched an expected single match query");
            }
        } else if (objectRoot instanceof Map) {
            Map<String, Object> recordValues = new HashMap();
            request.getFields().forEach((field) -> {
                try {
                    recordValues.put(field, JsonPath.parse(objectRoot).read(field));
                } catch (InvalidPathException e) {
                    recordValues.put(field, null);
                }
            });
            recordResult = new Record(recordValues);
        }

        return recordResult;

    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {

        NagiosQualificationParser nagiosParser = new NagiosQualificationParser();
        String jsonRootPath = NagiosQualificationParser.jsonPathMapping.get(request.getStructure());
        
        String jsonResponse = nagiosQuery("search", request, nagiosParser);

        List<Record> recordList = new ArrayList<>();
        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Map<String,String> metadata = new LinkedHashMap<>();
        metadata.put("count",JsonPath.parse(jsonResponse).read("$.hits.total", String.class));

        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            metadata.put("size", String.valueOf(listRoot.size()));
            listRoot.stream().map((arrayElement) -> {
                Map<String, Object> recordValues = new HashMap();
                DocumentContext jsonObject = JsonPath.parse(arrayElement);
                request.getFields().forEach((field) -> {
                    try {
                        recordValues.put(field, jsonObject.read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                });
                return recordValues;
            }).forEachOrdered((recordValues) -> {
                recordList.add(new Record(recordValues));
            });
        } else if (objectRoot instanceof Map) {
            metadata.put("size", "1");
            Map<String, Object> recordValues = new HashMap();
            DocumentContext jsonObject = JsonPath.parse(objectRoot);
            request.getFields().forEach((field) -> {
                recordValues.put(field, jsonObject.read(field));
            });
            recordList.add(new Record(recordValues));
        }

        return new RecordList(request.getFields(), recordList, metadata);

    }


    /*----------------------------------------------------------------------------------------------
     * PUBLIC HELPER METHODS
     *--------------------------------------------------------------------------------------------*/    
    public String buildUrl(String queryMethod, BridgeRequest request, NagiosQualificationParser nagiosParser) throws BridgeError {

        Map<String,String> metadata = BridgeUtils.normalizePaginationMetadata(request.getMetadata());
        String pageSize = "1000";
        String offset = "0";

        if (StringUtils.isNotBlank(metadata.get("pageSize")) && metadata.get("pageSize").equals("0") == false) {
            pageSize = metadata.get("pageSize");
        }
        if (StringUtils.isNotBlank(metadata.get("offset"))) {
            offset = metadata.get("offset");
        }

        String query = null;
        query = nagiosParser.parse(request.getQuery(),request.getParameters());

        // Build up the url that you will use to retrieve the source data. Use the query variable
        // instead of request.getQuery() to get a query without parameter placeholders.
        StringBuilder url = new StringBuilder();
        url.append(this.apiEndpoint)
            .append("/")
            .append(request.getStructure());
        
        addParameter(url, "apikey", request.getApplicationParameter(apiKey));

        //only set pagination if we're not counting.
        if (queryMethod.equals("count") == false) {
            addParameter(url, "records", pageSize + ":" + offset);
            //only set sorting if we're not counting *and* the request specified a sort order.
            if (request.getMetadata("order") != null) {
                List<String> orderList = new ArrayList<>();
                //loop over every defined sort order and add them to the Nagios URL
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(request.getMetadata("order")).entrySet()) {
                    String key = entry.getKey();
                    if (entry.getValue().equals("DESC")) {
                        orderList.add(String.format("%s:d", key));
                    }
                    else {
                        orderList.add(String.format("%s:a", key));
                    }
                }
                String order = StringUtils.join(orderList,",");
                addParameter(url, "orderby", order);
            }

        }

        LOGGER.trace("Nagios URL: {}", url.toString());
        return url.toString();

    }

    public HttpEntity buildRequestBody(String queryMethod, BridgeRequest request, NagiosQualificationParser nagiosParser) throws BridgeError {
        List<NameValuePair> params = new ArrayList<>();
        HttpEntity result = null;

        String query = nagiosParser.parse(request.getQuery(),request.getParameters());
        //Set query to return everything if no qualification defined.
        if (StringUtils.isBlank(query)) {
            query = "*:*";
        }

        // If the query is a JSON object...
        if (query.matches("^\\s*\\{.*?\\}\\s*$")) {
            params.add(new BasicNameValuePair("json", query));
            LOGGER.trace(String.format("JSON Query being sent to solr: %s", query));
        } else {
            params.add(new BasicNameValuePair("q", query));
            LOGGER.trace(String.format("Lucene Query being sent to solr: %s", query));
        }

        //only set sorting and field return limitation if we're not counting.
        if (queryMethod.equals("count") == false) {

            //only set field limitation if we're not counting *and* the request specified fields to be returned.
            if (request.getFields() != null && request.getFields().isEmpty() == false) {
                StringBuilder includedFields = new StringBuilder();
                String[] bridgeFields = request.getFieldArray();
                for (int i = 0; i < request.getFieldArray().length; i++) {
                    //strip _source from the beginning of the specified field name as this is redundent to Solr.
                    includedFields.append(bridgeFields[i]);
                    //only append a comma if this is not the last field
                    if (i != (request.getFieldArray().length -1)) {
                        includedFields.append(",");
                    }
                }
                params.add(new BasicNameValuePair("fl", includedFields.toString()));
            }
            //only set sorting if we're not counting *and* the request specified a sort order.
            if (request.getMetadata("order") != null) {
                List<String> orderList = new ArrayList<>();
                //loop over every defined sort order and add them to the Nagios URL
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(request.getMetadata("order")).entrySet()) {
                    String key = entry.getKey();
                    if (entry.getValue().equals("DESC")) {
                        orderList.add(String.format("%s desc", key));
                    }
                    else {
                        orderList.add(String.format("%s asc", key));
                    }
                }
                params.add(
                    new BasicNameValuePair(
                        "sort",
                        StringUtils.join(orderList,",")
                    )
                );
            }

        }

        try {
            result = new UrlEncodedFormEntity(params);
        } catch (UnsupportedEncodingException exceptionDetails) {
            throw new BridgeError (
                "Unable to generate the URL encoded HTTP Request body for the Solr API request.",
                exceptionDetails
            );
        }

        return result;
    }
    
    
    /*----------------------------------------------------------------------------------------------
     * PRIVATE HELPER METHODS
     *--------------------------------------------------------------------------------------------*/
    private void addParameter(StringBuilder url, String parameterName, String parameterValue) {
        if (url.toString().contains("?") == false) {
            url.append("?");
        } else {
            url.append("&");
        }
        url.append(URLEncoder.encode(parameterName))
            .append("=")
            .append(URLEncoder.encode(parameterValue));
    }
    
    private String nagiosQuery(String queryMethod, BridgeRequest request, NagiosQualificationParser nagiosParser) throws BridgeError{

        String result = null;
        String url = buildUrl(queryMethod, request, nagiosParser);
        
        // Initialize the HTTP Client, Response, and Get objects.
        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        HttpGetWithEntity get = new HttpGetWithEntity();
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new BridgeError(e);
        }
        get.setURI(uri);

        // Make the call to the REST source to retrieve data and convert the response from an
        // HttpEntity object into a Java string so more response parsing can be done.
        try {
            response = client.execute(get);
            Integer responseStatus = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();
            if (responseStatus >= 300 || responseStatus < 200) {
                String errorMessage = EntityUtils.toString(entity);
                throw new BridgeError(
                    String.format(
                        "The Nagios server returned a HTTP status code of %d, 200 was expected. Response body: %s",
                        responseStatus,
                        errorMessage
                    )
                );
            }

            result = EntityUtils.toString(entity);
            LOGGER.trace(String.format("Request response code: %s", response.getStatusLine().getStatusCode()));
        } catch (IOException e) {
            throw new BridgeError("Unable to make a connection to the Nagios server", e);
        }
        LOGGER.trace(String.format("Nagios response - Raw Output: %s", result));

        return result;
    }

    //TODO: REWRITE THIS FOR NAGIOS INSTEAD OF ELASTICSEARCH.
    private void testAuthenticationValues(String restEndpoint, String apiKey) throws BridgeError {
        LOGGER.debug("Testing the authentication credentials");
        HttpGetWithEntity get = new HttpGetWithEntity();
        URI uri;
        try {
            uri = new URI(String.format("%s/_cat/health",restEndpoint));
        } catch (URISyntaxException e) {
            throw new BridgeError(e);
        }
        get.setURI(uri);

        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            Integer responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == 401) {
                throw new BridgeError("Unauthorized: The inputted Username/Password combination is not valid.");
            }
            if (responseCode < 200 || responseCode >= 300) {
                throw new BridgeError(
                    String.format(
                        "Unsuccessful HTTP response - the server returned a %s status code, expected 200.",
                        responseCode
                    )
                );
            }
        }
        catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Nagios health check API.");
        }
    }

}