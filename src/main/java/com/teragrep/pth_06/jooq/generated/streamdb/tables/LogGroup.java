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
/*
 * This file is generated by jOOQ.
 */
package com.teragrep.pth_06.jooq.generated.streamdb.tables;


import com.teragrep.pth_06.jooq.generated.streamdb.Indexes;
import com.teragrep.pth_06.jooq.generated.streamdb.Keys;
import com.teragrep.pth_06.jooq.generated.streamdb.Streamdb;
import com.teragrep.pth_06.jooq.generated.streamdb.tables.records.LogGroupRecord;

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
public class LogGroup extends TableImpl<LogGroupRecord> {

    private static final long serialVersionUID = -668962726;

    /**
     * The reference instance of <code>streamdb.log_group</code>
     */
    public static final LogGroup LOG_GROUP = new LogGroup();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<LogGroupRecord> getRecordType() {
        return LogGroupRecord.class;
    }

    /**
     * The column <code>streamdb.log_group.id</code>.
     */
    public final TableField<LogGroupRecord, UInteger> ID = createField(DSL.name("id"), org.jooq.impl.SQLDataType.INTEGERUNSIGNED.nullable(false).identity(true), this, "");

    /**
     * The column <code>streamdb.log_group.name</code>.
     */
    public final TableField<LogGroupRecord, String> NAME = createField(DSL.name("name"), org.jooq.impl.SQLDataType.VARCHAR(100).nullable(false), this, "");

    /**
     * Create a <code>streamdb.log_group</code> table reference
     */
    public LogGroup() {
        this(DSL.name("log_group"), null);
    }

    /**
     * Create an aliased <code>streamdb.log_group</code> table reference
     */
    public LogGroup(String alias) {
        this(DSL.name(alias), LOG_GROUP);
    }

    /**
     * Create an aliased <code>streamdb.log_group</code> table reference
     */
    public LogGroup(Name alias) {
        this(alias, LOG_GROUP);
    }

    private LogGroup(Name alias, Table<LogGroupRecord> aliased) {
        this(alias, aliased, null);
    }

    private LogGroup(Name alias, Table<LogGroupRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""));
    }

    public <O extends Record> LogGroup(Table<O> child, ForeignKey<O, LogGroupRecord> key) {
        super(child, key, LOG_GROUP);
    }

    @Override
    public Schema getSchema() {
        return Streamdb.STREAMDB;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.<Index>asList(Indexes.LOG_GROUP_PRIMARY);
    }

    @Override
    public Identity<LogGroupRecord, UInteger> getIdentity() {
        return Keys.IDENTITY_LOG_GROUP;
    }

    @Override
    public UniqueKey<LogGroupRecord> getPrimaryKey() {
        return Keys.KEY_LOG_GROUP_PRIMARY;
    }

    @Override
    public List<UniqueKey<LogGroupRecord>> getKeys() {
        return Arrays.<UniqueKey<LogGroupRecord>>asList(Keys.KEY_LOG_GROUP_PRIMARY);
    }

    @Override
    public LogGroup as(String alias) {
        return new LogGroup(DSL.name(alias), this);
    }

    @Override
    public LogGroup as(Name alias) {
        return new LogGroup(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public LogGroup rename(String name) {
        return new LogGroup(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public LogGroup rename(Name name) {
        return new LogGroup(name, null);
    }

    // -------------------------------------------------------------------------
    // Row2 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row2<UInteger, String> fieldsRow() {
        return (Row2) super.fieldsRow();
    }
}
