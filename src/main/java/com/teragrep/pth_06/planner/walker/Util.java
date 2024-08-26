/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
package com.teragrep.pth_06.planner.walker;

import org.w3c.dom.Element;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>Util</h1> Class for holding various util methods used by the walkers.
 *
 * @since 23/09/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 */
public class Util {

    public static String stripQuotes(String value) {
        Matcher m = Pattern.compile("^\"(.*)\"$").matcher(value);
        Matcher m1 = Pattern.compile("^'(.*)'$").matcher(value);

        String strUnquoted = value;
        // check "-quotes
        if (m.find()) {
            strUnquoted = m.group(1);
        }
        else {
            // check '-quotes
            if (m1.find()) {
                strUnquoted = m1.group(1);
            }
        }
        return strUnquoted;
    }

    public static String addQuotes(String s) {
        String str = s;
        // If quotes are missing, add them
        if (!s.startsWith("\"") && !s.startsWith("'")) {
            str = "\"".concat(s);
        }
        if (!s.endsWith("\"") && !s.endsWith("'")) {
            str = str.concat("\"");
        }
        return str;
    }

    // Time utility methods
    /**
     * Calculate epoch time from relative time modifier. IE. now()- time range
     */
    public static long relativeTimeModifier(Timestamp timestamp, String value) {
        Instant rv = null;
        Matcher matcher = Pattern.compile("^-?\\d+").matcher(value);

        if (!matcher.find())
            throw new NumberFormatException("Unknown relative time modifier string [" + value + "]");

        long v = Long.parseLong(matcher.group());
        matcher.group().length();
        String unit = value.substring(matcher.group().length(), value.length());

        LocalDateTime now = timestamp.toLocalDateTime();
        rv = timestamp.toInstant();
        if (
            "s".equalsIgnoreCase(unit) || "sec".equalsIgnoreCase(unit) || "secs".equalsIgnoreCase(unit)
                    || "second".equalsIgnoreCase(unit) || "seconds".equalsIgnoreCase(unit)
        ) {
            rv = rv.plusSeconds(v);
        }
        if (
            "m".equalsIgnoreCase(unit) || "min".equalsIgnoreCase(unit) || "minute".equalsIgnoreCase(unit)
                    || "minutes".equalsIgnoreCase(unit)
        ) {
            rv = rv.plus(v, ChronoUnit.MINUTES);
        }
        if (
            "h".equalsIgnoreCase(unit) || "hr".equalsIgnoreCase(unit) || "hrs".equalsIgnoreCase(unit)
                    || "hour".equalsIgnoreCase(unit) || "hours".equalsIgnoreCase(unit)
        ) {
            rv = rv.plus(v, ChronoUnit.HOURS);
        }
        if ("d".equalsIgnoreCase(unit) || "day".equalsIgnoreCase(unit) || "days".equalsIgnoreCase(unit)) {
            rv = rv.plus(v, ChronoUnit.DAYS);
        }
        // longer times are not supported with Instant, so we need to change it into the
        // localdatetime object
        if ("w".equalsIgnoreCase(unit) || "week".equalsIgnoreCase(unit) || "weeks".equalsIgnoreCase(unit)) {
            now = now.plusWeeks(v);
            rv = now.toInstant(ZoneOffset.UTC);
        }
        if ("mon".equalsIgnoreCase(unit) || "month".equalsIgnoreCase(unit) || "months".equalsIgnoreCase(unit)) {
            now = now.plusMonths(v);
            rv = now.toInstant(ZoneOffset.UTC);
        }
        if (
            "y".equalsIgnoreCase(unit) || "yr".equalsIgnoreCase(unit) || "yrs".equalsIgnoreCase(unit)
                    || "year".equalsIgnoreCase(unit) || "years".equalsIgnoreCase(unit)
        ) {
            now = now.plusYears(v);
            rv = now.toInstant(ZoneOffset.UTC);
        }
        // System.out.println("Relative time (UTC)=" + rv.atZone(ZoneOffset.UTC) + "
        // unixtime=" + rv.getEpochSecond());
        return rv.getEpochSecond();
    }

    public static long unixEpochFromString(String timevalue, String tf) {
        long rv = 0;
        // Remove '"' around string if needed
        timevalue = stripQuotes(timevalue);
        try {
            if (tf != null && !tf.isEmpty()) {
                if ("%s".equals(tf)) {
                    // unix-epoch string so
                    long epoch = Long.parseLong(timevalue);
                    rv = epoch;
                }
                else {
                    // Not yet implemented so use default
                    LocalDateTime time = LocalDateTime
                            .parse(timevalue, DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss"));
                    final ZonedDateTime zonedtime = ZonedDateTime.of(time, ZoneId.systemDefault()); // .atZone(fromZone);
                    final ZonedDateTime converted = zonedtime.withZoneSameInstant(ZoneOffset.UTC);
                    rv = converted.toEpochSecond();
                    //System.out.println("LocalDatetime="+time+ " TZ="+ZoneId.systemDefault());
                    //System.out.println("Datetime(UTC)="+converted+" timestamp="+rv);
                }
            }
            else {
                // Use default format
                LocalDateTime time = LocalDateTime.parse(timevalue, DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss"));
                final ZonedDateTime zonedtime = ZonedDateTime.of(time, ZoneId.systemDefault()); // .atZone(fromZone);
                final ZonedDateTime converted = zonedtime.withZoneSameInstant(ZoneOffset.UTC);
                rv = converted.toEpochSecond();
                //System.out.println("LocalDatetime="+time+ " TZ="+ZoneId.systemDefault());
                //System.out.println("Datetime(UTC)="+converted+" timestamp="+rv);
            }
        }
        catch (NumberFormatException exn) {
            exn.printStackTrace();
            throw new RuntimeException("TimeQualifier conversion error: <" + timevalue + "> can't be parsed.");
        }
        catch (DateTimeParseException ex) {
            ex.printStackTrace();
            throw new RuntimeException("TimeQualifier conversion error: <" + timevalue + "> can't be parsed.");
        }
        return rv;
    }

    // xml-util
    public static String elementAsString(Element el) {
        String str = null;
        try {
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            StringWriter buffer = new StringWriter();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.transform(new DOMSource(el), new StreamResult(buffer));
            str = buffer.toString();
        }
        catch (TransformerConfigurationException tex) {
        }
        catch (TransformerException ex) {
        }
        return str;
    }

}
