package com.kineticdata.bridgehub.adapter.nagios;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.QualificationParser;
import java.net.URLEncoder;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

public class NagiosQualificationParser extends QualificationParser {
    
    public static String PARAMETER_PATTERN_JSON_SAFE = "<%= parameter\\['(.*?)'\\] %>";
    public static String PARAMETER_PATTERN_GROUP_MATCH = "<%=\\s*parameter\\[\\s*\"(.*?)\"\\s*\\]\\s*%>";
    
    public static final Map<String, String> jsonPathMapping = Collections
        .unmodifiableMap(
            Stream.of(
                new SimpleEntry<>("objects/hoststatus", "$.hostsatuslist.hoststatus"),
                new SimpleEntry<>("objects/servicestatus", "$.servicestatuslist.servicestatus"),
                new SimpleEntry<>("objects/logentries", ""),
                new SimpleEntry<>("objects/statehistory", ""),
                new SimpleEntry<>("objects/comment", ""),
                new SimpleEntry<>("objects/downtime", ""),
                new SimpleEntry<>("objects/contact", ""),
                new SimpleEntry<>("objects/host", ""),
                new SimpleEntry<>("objects/service", ""),
                new SimpleEntry<>("objects/hostgroup", ""),
                new SimpleEntry<>("objects/servicegroup", ""),
                new SimpleEntry<>("objects/contactgroup", ""),
                new SimpleEntry<>("objects/hostgroupmembers", ""),
                new SimpleEntry<>("objects/servicegroupmembers", ""),
                new SimpleEntry<>("objects/contactgroupmembers", ""),
                new SimpleEntry<>("objects/rrdexport", ""),
                new SimpleEntry<>("objects/cpexport", "")
            ).collect(
                Collectors.toMap(
                    (e) -> e.getKey(),
                    (e) -> e.getValue()
                )
            )
        );
    
    @Override
    public String encodeParameter(String name, String value) {
        String result = null;
        //http://lucene.apache.org/core/4_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters
        //Next three lines: escape the following characters with a backslash: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /  
        String regexReservedCharactersPattern = "(\\*|\\+|\\-|\\=|\\~|\\\"|\\?|\\^|\\$|\\{|\\}|\\(|\\)|\\:|\\!|\\/|\\[|\\]|\\\\|\\s)";
        if (StringUtils.isNotEmpty(value)) {
            result = value.replaceAll(regexReservedCharactersPattern, Matcher.quoteReplacement("\\") + "$1")
                .replaceAll("\\|\\|", "\\\\||")
                .replaceAll("\\&\\&", "\\\\&&")
                .replaceAll("\\b+AND\\b+", Matcher.quoteReplacement("\\\\AND"))
                .replaceAll("\\b+OR\\b+", Matcher.quoteReplacement("\\\\OR"))
                .replaceAll("\\b+NOT\\b+", Matcher.quoteReplacement("\\\\NOT"));
        }
        return result;
    }
    
    @Override
    public String parse(String jsonQuery, Map<String, String> parameters) throws BridgeError {
        
        StringBuffer resultBuffer = new StringBuffer();
        Pattern pattern = Pattern.compile(PARAMETER_PATTERN_JSON_SAFE);
        Map<String, String[]> jsonObject = null;
                
        try {
            jsonObject = (Map<String, String[]>)JSONValue.parseWithException(jsonQuery);
        } catch (ParseException exceptionDetails) {
            throw new BridgeError(
                String.format("The bridge query (%s) appears to be a JSON string " +
                "because it starts and ends with curly braces but " +
                " the query failed to parse successfully as JSON.", jsonQuery),
                exceptionDetails
            );
        }
        
        for (Map.Entry<String, String[]> entry : jsonObject.entrySet()) {
            for (String value : entry.getValue()) {
                
                String urlParamName = URLEncoder.encode(entry.getKey());
                
                resultBuffer.append(urlParamName);
                resultBuffer.append("=");
                
                Matcher matcher = pattern.matcher(value);

                while (matcher.find()) {
                    // Retrieve the necessary values
                    String parameterName = matcher.group(1);
                    // If there were no parameters provided
                    if (parameters == null) {
                        throw new BridgeError("Unable to parse qualification, "+
                            "the '"+parameterName+"' parameter was referenced but no "+
                            "parameters were provided.");
                    }
                    String parameterValue = parameters.get(parameterName);
                    // If there is a reference to a parameter that was not passed
                    if (parameterValue == null) {
                        throw new BridgeError("Unable to parse qualification, "+
                            "the '"+parameterName+"' parameter was referenced but "+
                            "not provided.");
                    }

                    String urlParamValue = JSONValue.escape(parameterValue);
                    matcher.appendReplacement(
                        resultBuffer,
                        Matcher.quoteReplacement(urlParamValue)
                    );
                }

                matcher.appendTail(resultBuffer);
                resultBuffer.append("&");
            }
        }
        
        //Remove trailing &
        return StringUtils.chomp(resultBuffer.toString(), "&");

    }
        
}
