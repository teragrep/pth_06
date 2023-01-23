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
/*
 * This file is generated by jOOQ.
 */
package com.teragrep.pth06.jooq.generated.streamdb.tables.records;


import com.teragrep.pth06.jooq.generated.streamdb.tables.Stream;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;
import org.jooq.types.UInteger;


/**
 * This class is generated by jOOQ.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.12.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class StreamRecord extends UpdatableRecordImpl<StreamRecord> implements Record5<UInteger, UInteger, String, String, String> {

    private static final long serialVersionUID = 1864523725;

    /**
     * Setter for <code>streamdb.stream.id</code>.
     */
    public void setId(UInteger value) {
        set(0, value);
    }

    /**
     * Getter for <code>streamdb.stream.id</code>.
     */
    public UInteger getId() {
        return (UInteger) get(0);
    }

    /**
     * Setter for <code>streamdb.stream.gid</code>.
     */
    public void setGid(UInteger value) {
        set(1, value);
    }

    /**
     * Getter for <code>streamdb.stream.gid</code>.
     */
    public UInteger getGid() {
        return (UInteger) get(1);
    }

    /**
     * Setter for <code>streamdb.stream.directory</code>.
     */
    public void setDirectory(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>streamdb.stream.directory</code>.
     */
    public String getDirectory() {
        return (String) get(2);
    }

    /**
     * Setter for <code>streamdb.stream.stream</code>.
     */
    public void setStream(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>streamdb.stream.stream</code>.
     */
    public String getStream() {
        return (String) get(3);
    }

    /**
     * Setter for <code>streamdb.stream.tag</code>.
     */
    public void setTag(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>streamdb.stream.tag</code>.
     */
    public String getTag() {
        return (String) get(4);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<UInteger> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record5 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row5<UInteger, UInteger, String, String, String> fieldsRow() {
        return (Row5) super.fieldsRow();
    }

    @Override
    public Row5<UInteger, UInteger, String, String, String> valuesRow() {
        return (Row5) super.valuesRow();
    }

    @Override
    public Field<UInteger> field1() {
        return Stream.STREAM.ID;
    }

    @Override
    public Field<UInteger> field2() {
        return Stream.STREAM.GID;
    }

    @Override
    public Field<String> field3() {
        return Stream.STREAM.DIRECTORY;
    }

    @Override
    public Field<String> field4() {
        return Stream.STREAM.STREAM_;
    }

    @Override
    public Field<String> field5() {
        return Stream.STREAM.TAG;
    }

    @Override
    public UInteger component1() {
        return getId();
    }

    @Override
    public UInteger component2() {
        return getGid();
    }

    @Override
    public String component3() {
        return getDirectory();
    }

    @Override
    public String component4() {
        return getStream();
    }

    @Override
    public String component5() {
        return getTag();
    }

    @Override
    public UInteger value1() {
        return getId();
    }

    @Override
    public UInteger value2() {
        return getGid();
    }

    @Override
    public String value3() {
        return getDirectory();
    }

    @Override
    public String value4() {
        return getStream();
    }

    @Override
    public String value5() {
        return getTag();
    }

    @Override
    public StreamRecord value1(UInteger value) {
        setId(value);
        return this;
    }

    @Override
    public StreamRecord value2(UInteger value) {
        setGid(value);
        return this;
    }

    @Override
    public StreamRecord value3(String value) {
        setDirectory(value);
        return this;
    }

    @Override
    public StreamRecord value4(String value) {
        setStream(value);
        return this;
    }

    @Override
    public StreamRecord value5(String value) {
        setTag(value);
        return this;
    }

    @Override
    public StreamRecord values(UInteger value1, UInteger value2, String value3, String value4, String value5) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached StreamRecord
     */
    public StreamRecord() {
        super(Stream.STREAM);
    }

    /**
     * Create a detached, initialised StreamRecord
     */
    public StreamRecord(UInteger id, UInteger gid, String directory, String stream, String tag) {
        super(Stream.STREAM);

        set(0, id);
        set(1, gid);
        set(2, directory);
        set(3, stream);
        set(4, tag);
    }
}
