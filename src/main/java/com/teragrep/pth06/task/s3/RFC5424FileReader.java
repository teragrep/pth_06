package com.teragrep.pth06.task.s3;

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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

// AWS-client
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;

// logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// RFC-5424-parser
import com.teragrep.rlo_06.ParserResultset;
import com.teragrep.rlo_06.RFC5424Parser;
import com.teragrep.rlo_06.ParserEnum;
import com.teragrep.rlo_06.RFC5424ParserSDSubscription;
import com.teragrep.rlo_06.RFC5424ParserSubscription;
import com.teragrep.rlo_06.ParseException;

/**
 * <h2>RFC5424 File Reader</h2>
 *
 * Class for reading RFC5424 files.
 *
 * @since 12/03/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 */
public class RFC5424FileReader {
    final Logger LOGGER = LoggerFactory.getLogger(RFC5424FileReader.class);
    private final AmazonS3 s3client;
    private String logName = null;
    private final boolean skipNonRFC5424Files;

    //LogParser
    RFC5424Parser parser = null;

    // Actual s3-connection     
    BufferedInputStream inputStream = null;
    ParserResultset currentResultSet = null;

    boolean isOpen = false;

    public RFC5424FileReader(AmazonS3 s3client, boolean skipNonRFC5424Files){
        this.s3client = s3client;
        this.skipNonRFC5424Files = skipNonRFC5424Files;
    }

    public void open(String bucketName, String keyName) throws IOException{
        this.logName = bucketName + "/" + keyName;
        S3Object s3object = null;
        try {
            LOGGER.debug("Attempting to open file: " + logName);
            s3object = s3client.getObject(bucketName, keyName);
        }
        catch (AmazonServiceException amazonServiceException) {

            if (403 == amazonServiceException.getStatusCode()) {
                LOGGER.error("Skipping file " + logName + " due to errorCode:" +
                        amazonServiceException.getStatusCode());
            }
            else {
                throw amazonServiceException;
            }
        }

        if(s3object != null ){
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug("Open S3 stream bucket:" + bucketName + " keyname=" + keyName + " Metadata length:" + s3object.getObjectMetadata().getContentLength());
            }
            inputStream = new BufferedInputStream(s3object.getObjectContent(), 8*1024*1024);
            GZIPInputStream in = new GZIPInputStream(inputStream);
            isOpen = true;

            // Define fields we are interested
            // Main level
            RFC5424ParserSubscription subscription = new RFC5424ParserSubscription();
            // subscription.add(ParserEnum.PRIORITY);
            // subscription.add(ParserEnum.VERSION);
            subscription.add(ParserEnum.TIMESTAMP);
            // subscription.add(ParserEnum.HOSTNAME);
            // subscription.add(ParserEnum.APPNAME);
            // subscription.add(ParserEnum.PROCID);
            // subscription.add(ParserEnum.MSGID);
            subscription.add(ParserEnum.MSG);

            // Structured
            RFC5424ParserSDSubscription sdSubscription = new RFC5424ParserSDSubscription();
            sdSubscription.subscribeElement("event_node_source@48577","source");
            sdSubscription.subscribeElement("event_node_relay@48577","source");
            sdSubscription.subscribeElement("event_node_source@48577","source_module");
            sdSubscription.subscribeElement("event_node_relay@48577","source_module");
            sdSubscription.subscribeElement("event_node_source@48577","hostname");
            sdSubscription.subscribeElement("event_node_relay@48577","hostname");

            // Origin
            sdSubscription.subscribeElement("origin@48577","hostname");

            // workaround cfe-06/issue/68 by subscribing the broken field
            sdSubscription.subscribeElement("rfc3164@48577","syslogtag");

            // Set up result
            currentResultSet = new ParserResultset(subscription, sdSubscription);
            LOGGER.trace("S3FileHandler.open() Initialized result set with element lists");
            // Initialize parser with inputStream
            // Initialize parser with GZip-inputStream
            parser = new RFC5424Parser(in);
            LOGGER.info("S3FileHandler.open() Initialized parser for: " + logName);
        } else {
            isOpen = false;
        }
    }

    public boolean hasNext() throws IOException{
        if (isOpen) {
            boolean hasNext;
            if (currentResultSet != null)
                currentResultSet.clear(); // Empty set
            else
                LOGGER.warn("Trying to clear empty result set");
            try {
                hasNext = parser.next(currentResultSet);
            } catch (ParseException parseException) {
                LOGGER.error("ParseException at object: " + logName);
                if (skipNonRFC5424Files) {
                    return false;
                }
                else {
                    throw parseException;
                }
            }
            //            if(LOGGER.isTraceEnabled())
            LOGGER.trace("S3FileHandler.hasNext() stream:" + logName + "\nhasNextLine:" + hasNext);
            return hasNext;
        }
        else {
            return false;
        }
    }

    public ParserResultset next() {
        return currentResultSet;
    }

    public void close() throws IOException{
        isOpen = false;
        //br.close();
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
