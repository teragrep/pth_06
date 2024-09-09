/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022, 2023, 2024 Suomen Kanuuna Oy
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
package com.teragrep.pth_06.jooq.generated.journaldb.tables;


import com.teragrep.pth_06.jooq.generated.journaldb.Indexes;
import com.teragrep.pth_06.jooq.generated.journaldb.Journaldb;
import com.teragrep.pth_06.jooq.generated.journaldb.Keys;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.records.HostRecord;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.Index;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row2;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.jooq.types.UShort;


/**
 * Host names
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.12.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Host extends TableImpl<HostRecord> {

    private static final long serialVersionUID = -1172075105;

    /**
     * The reference instance of <code>journaldb.host</code>
     */
    public static final Host HOST = new Host();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<HostRecord> getRecordType() {
        return HostRecord.class;
    }

    /**
     * The column <code>journaldb.host.id</code>.
     */
    public final TableField<HostRecord, UShort> ID = createField(DSL.name("id"), org.jooq.impl.SQLDataType.SMALLINTUNSIGNED.nullable(false).identity(true), this, "");

    /**
     * The column <code>journaldb.host.name</code>. Name of the host
     */
    public final TableField<HostRecord, String> NAME = createField(DSL.name("name"), org.jooq.impl.SQLDataType.VARCHAR(175).nullable(false), this, "Name of the host");

    /**
     * Create a <code>journaldb.host</code> table reference
     */
    public Host() {
        this(DSL.name("host"), null);
    }

    /**
     * Create an aliased <code>journaldb.host</code> table reference
     */
    public Host(String alias) {
        this(DSL.name(alias), HOST);
    }

    /**
     * Create an aliased <code>journaldb.host</code> table reference
     */
    public Host(Name alias) {
        this(alias, HOST);
    }

    private Host(Name alias, Table<HostRecord> aliased) {
        this(alias, aliased, null);
    }

    private Host(Name alias, Table<HostRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment("Host names"));
    }

    public <O extends Record> Host(Table<O> child, ForeignKey<O, HostRecord> key) {
        super(child, key, HOST);
    }

    @Override
    public Schema getSchema() {
        return Journaldb.JOURNALDB;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.<Index>asList(Indexes.HOST_PRIMARY, Indexes.HOST_UIX_HOST_NAME);
    }

    @Override
    public Identity<HostRecord, UShort> getIdentity() {
        return Keys.IDENTITY_HOST;
    }

    @Override
    public UniqueKey<HostRecord> getPrimaryKey() {
        return Keys.KEY_HOST_PRIMARY;
    }

    @Override
    public List<UniqueKey<HostRecord>> getKeys() {
        return Arrays.<UniqueKey<HostRecord>>asList(Keys.KEY_HOST_PRIMARY, Keys.KEY_HOST_UIX_HOST_NAME);
    }

    @Override
    public Host as(String alias) {
        return new Host(DSL.name(alias), this);
    }

    @Override
    public Host as(Name alias) {
        return new Host(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public Host rename(String name) {
        return new Host(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public Host rename(Name name) {
        return new Host(name, null);
    }

    // -------------------------------------------------------------------------
    // Row2 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row2<UShort, String> fieldsRow() {
        return (Row2) super.fieldsRow();
    }
}
