package com.teragrep.pth_06.planner.walker;

import com.teragrep.jue_01.GlobToRegEx;
import org.w3c.dom.Element;

// HDFS planner walker, only aims to filter out the topics as the planner only outputs the metadata for AVRO-files containing the records. The rest of the condition handling is done in the separate tasker walker.
public class HdfsConditionWalker extends XmlWalker<String>{

    @Override
    String emitElem(Element current) {
        String tag = current.getTagName();
        String value = current.getAttribute("value");
        String operation = current.getAttribute("operation");

        String queryCondition = null;
        // only index equals supported
        if (tag.equalsIgnoreCase("index")) {
            if (operation.equalsIgnoreCase("EQUALS")) {
                queryCondition = GlobToRegEx.regexify(value);
            }
        }
        return queryCondition;
    }

    public String fromString(String inXml) throws Exception {
        return super.fromString(inXml);
    }

    @Override
    String emitLogicalOperation(String op, Object l, Object r) throws Exception {
        String left = (String) l;
        String right = (String) r;

        String rv = null;
        /*
        index can not have two values at the same go therefore "AND".equals(op)
        is not implemented
         */
        if ("OR".equals(op)) {
            rv = "(" + left + "|" + right + ")";
        }
        return rv;
    }

    @Override
    String emitUnaryOperation(String op, Element current) throws Exception {
        // NOT is a filter, not a topic matcher
        return null;
    }
}