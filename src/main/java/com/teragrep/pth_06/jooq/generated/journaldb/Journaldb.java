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
package com.teragrep.pth_06.jooq.generated.journaldb;


import com.teragrep.pth_06.jooq.generated.DefaultCatalog;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.Bucket;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.Host;
import com.teragrep.pth_06.jooq.generated.journaldb.tables.Logfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import org.jooq.Catalog;
import org.jooq.Table;
import org.jooq.impl.SchemaImpl;


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
public class Journaldb extends SchemaImpl {

    private static final long serialVersionUID = -1865582803;

    /**
     * The reference instance of <code>journaldb</code>
     */
    public static final Journaldb JOURNALDB = new Journaldb();

    /**
     * Buckets in object storage
     */
    public final Bucket BUCKET = com.teragrep.pth_06.jooq.generated.journaldb.tables.Bucket.BUCKET;

    /**
     * Host names
     */
    public final Host HOST = com.teragrep.pth_06.jooq.generated.journaldb.tables.Host.HOST;

    /**
     * Contains information for log files that have been run through Log Archiver
     */
    public final Logfile LOGFILE = com.teragrep.pth_06.jooq.generated.journaldb.tables.Logfile.LOGFILE;

    /**
     * No further instances allowed
     */
    private Journaldb() {
        super("journaldb", null);
    }


    @Override
    public Catalog getCatalog() {
        return DefaultCatalog.DEFAULT_CATALOG;
    }

    @Override
    public final List<Table<?>> getTables() {
        List result = new ArrayList();
        result.addAll(getTables0());
        return result;
    }

    private final List<Table<?>> getTables0() {
        return Arrays.<Table<?>>asList(
            Bucket.BUCKET,
            Host.HOST,
            Logfile.LOGFILE);
    }
}
