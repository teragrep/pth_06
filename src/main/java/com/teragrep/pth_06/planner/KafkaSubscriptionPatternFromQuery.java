package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.planner.walker.KafkaWalker;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.regex.Pattern;

public class KafkaSubscriptionPatternFromQuery {
    private final String query;

    public KafkaSubscriptionPatternFromQuery(final String query) {
        this.query = query;
    }

    public Pattern pattern() {
        String topicsRegexString;
            try {
                KafkaWalker parser = new KafkaWalker();
                topicsRegexString = parser.fromString(query);
            }
            catch (ParserConfigurationException | IOException | SAXException ex) {
                throw new RuntimeException(
                        "Exception building kafka pattern from query <" + query
                                + "> exception: " + ex
                );
            }
        if (topicsRegexString == null) {
            topicsRegexString = "^.*$"; // all topics if null
        }
        return Pattern.compile(topicsRegexString);
    }
}
