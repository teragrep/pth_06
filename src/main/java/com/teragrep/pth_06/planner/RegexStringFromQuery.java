package com.teragrep.pth_06.planner;

import com.teragrep.pth_06.planner.walker.KafkaWalker;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class RegexStringFromQuery {
    private final String query;

    public RegexStringFromQuery(final String query) {
        this.query = query;
    }

    public String regexString() {
        String topicsRegexString;
            try {
                KafkaWalker parser = new KafkaWalker();
                topicsRegexString = parser.fromString(query);
            }
            catch (ParserConfigurationException | IOException | SAXException ex) {
                throw new RuntimeException(
                        "Exception walking query <" + query
                                + "> exception:" + ex
                );
            }
        if (topicsRegexString == null) {
            topicsRegexString = "^.*$"; // all topics if null
        }
        return topicsRegexString;
    }
}
