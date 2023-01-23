/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.pth06.planner.walker;

import com.teragrep.pth06.planner.StreamDBClient;
import org.jooq.Condition;
import org.w3c.dom.Element;

import java.sql.Date;
import java.time.Instant;

import static com.teragrep.pth06.jooq.generated.journaldb.Journaldb.JOURNALDB;
import static com.teragrep.pth06.jooq.generated.streamdb.Streamdb.STREAMDB;

/**
 * <h1>Condition Walker</h1>
 *
 * Walker for conditions.
 *
 * @since 23/09/2021
 * @author Kimmo Leppinen
 * @author Mikko Kortelainen
 */
public class ConditionWalker extends XmlWalker {
    // Default query is full
    private boolean streamQuery = false;

    /**
     * Constructor without connection. Used during unit-tests. Enables jooq-query construction.
     */
    public ConditionWalker() {
        super();
    }


    public Condition fromString(String inXml, boolean streamQuery) throws Exception {
        this.streamQuery = streamQuery;
        return fromString(inXml);
    }

    @Override
    public Condition emitLogicalOperation(String op, Object l, Object r) throws Exception{
        Condition rv = null;
        Condition left =(Condition)l;
        Condition right =(Condition)r;
        //System.out.println("CondifionWalker.emitLogicalOperation incoming op:"+op+" left:"+left+" right:"+right);
        if(op == null){
            throw new Exception("Parse error, unbalanced elements. "+left.toString());
        }
        if(op.equalsIgnoreCase("AND")) {
            rv = left.and(right);
        } else if(op.equalsIgnoreCase("OR")){
            rv = left.or(right);
        } else if(op.equalsIgnoreCase("NOT")){
            rv = left.not();
        }
        else {
            throw new Exception("Parse error, unssorted logical operation. op:"+op+ " expression:"+left.toString());
        }
        return rv;
    }

    @Override
    public Condition emitUnaryOperation(String op, Element current) throws Exception{
        Condition rv = emitElem(current);
        System.out.println("ConditionWalker.emitUnaryOperation incoming op:"+op+" element:"+current);
        if(op == null){
            throw new Exception("Parse error, Unknown unary operation:"+op+" expression:"+rv.toString());
        }
        if(rv != null) {
            if(op.equalsIgnoreCase("NOT")){
                rv = rv.not();
            }
            else {
                throw new Exception("Parse error, unsupported logical operation. op:"+op+ " expression:"+rv.toString());
            }
        }
        return rv;
    }

    Condition emitElem(Element current) {
        String tag = current.getTagName();
        String value = current.getAttribute("value");
        String operation = current.getAttribute("operation");
        //System.out.println("StreamQuery="+streamQuery+" Node is terminal tag:" + tag + " val:" + value + " Operation:" + operation);
        Condition queryCondition = null;
        // directory
        if (tag.equalsIgnoreCase("index")) {
            if (operation != null) {
                if (streamQuery) {
                    queryCondition =
                            STREAMDB.STREAM.DIRECTORY.like(
                                    value.replace('*', '%')
                            );
                } else {
                    queryCondition =
                            StreamDBClient.GetArchivedObjectsFilterTable.directory.like(
                                    value.replace('*', '%').toLowerCase()
                            );
                }
                if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                    queryCondition = queryCondition.not();
                }
            }
        }
        // stream
        if (tag.equalsIgnoreCase("sourcetype")) {
            if (operation != null) {
                if (streamQuery) {
                    queryCondition = STREAMDB.STREAM.STREAM_.like(
                            value.replace('*', '%')
                    );
                } else {
                    queryCondition =
                            StreamDBClient.GetArchivedObjectsFilterTable.stream.like(
                                    value.replace('*', '%').toLowerCase()
                            );
                }
                if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                    queryCondition = queryCondition.not();
                }
            }
        }
        // host
        if (tag.equalsIgnoreCase("host")) {
            if (operation != null) {
                if (streamQuery) {
                    queryCondition = STREAMDB.HOST.NAME.like(
                            value.replace('*', '%')
                    );
                } else {
                    queryCondition =
                            StreamDBClient.GetArchivedObjectsFilterTable.host.like(
                            value.replace('*', '%').toLowerCase()
                    );
                }
                if (operation.equalsIgnoreCase("NOT_EQUALS")) {
                    queryCondition = queryCondition.not();
                }
            }
        }
        if (!streamQuery) {
            // Handle also timequalifiers
            if (tag.equalsIgnoreCase("earliest") || tag.equalsIgnoreCase("index_earliest")) {
                // SQL connection uses localTime in the session, so we use unix to come over the conversions
                // hour based files are being used so earliest needs conversion to the point of the last hour
                int earliestEpoch =  Integer.parseInt(value);
                int earliestEpochHour = earliestEpoch-earliestEpoch%3600;
                Instant instant = Instant.ofEpochSecond( earliestEpochHour );
                java.sql.Date timequalifier = new Date(instant.toEpochMilli());

                if (operation != null) {
                    queryCondition = JOURNALDB.LOGFILE.LOGDATE.greaterOrEqual(timequalifier);
                     /* not supported for mariadb
                        queryCondition = queryCondition.and(toTimestamp(
                                regexpReplaceAll(JOURNALDB.LOGFILE.PATH, "((^.*\\/.*-)|(\\.log\\.gz.*))", ""),
                                "YYYYMMDDHH24").greaterOrEqual(Timestamp.from(instant)));
                        */
                    // NOTE uses literal path
                    queryCondition = queryCondition.and("UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H'))" +
                            " >= " + instant.getEpochSecond());
                }
            }
            if (tag.equalsIgnoreCase("latest") || tag.equalsIgnoreCase("index_latest")) {
                // SQL connection uses localTime in the session, so we use unix to come over the conversions
                Instant instant = Instant.ofEpochSecond( Integer.parseInt(value) );
                java.sql.Date timequalifier = new Date(instant.toEpochMilli());

                if (operation != null) {
                    queryCondition = JOURNALDB.LOGFILE.LOGDATE.lessOrEqual(timequalifier);
                    /* not supported for mariadb
                        queryCondition = queryCondition.and(toTimestamp(
                                regexpReplaceAll(JOURNALDB.LOGFILE.PATH, "((^.*\\/.*-)|(\\.log\\.gz.*))", ""),
                                "YYYYMMDDHH24").lessOrEqual(Timestamp.from(instant)));
                    */
                    // NOTE uses literal path
                    /*
                     to match
                     2021/09-27/sc-99-99-14-244/messages/messages-2021092722.gz.4
                     2018/04-29/sc-99-99-14-245/f17/f17.logGLOB-2018042900.log.gz
                     */

                    queryCondition = queryCondition.and("UNIX_TIMESTAMP(STR_TO_DATE(SUBSTRING(REGEXP_SUBSTR(path,'[0-9]+(\\.log)?\\.gz(\\.[0-9]*)?$'), 1, 10), '%Y%m%d%H'))" +
                            " <= " + instant.getEpochSecond());
                }
            }
        }

//            if(queryCondition != null)
//                System.out.println("ConditionWalker parseElem:"+queryCondition.toString());
        return queryCondition;
    }
}