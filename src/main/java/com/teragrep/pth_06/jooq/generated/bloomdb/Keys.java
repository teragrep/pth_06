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
package com.teragrep.pth_06.jooq.generated.bloomdb;


import com.teragrep.pth_06.jooq.generated.bloomdb.tables.Filtertype;
import com.teragrep.pth_06.jooq.generated.bloomdb.tables.records.FiltertypeRecord;

import javax.annotation.Generated;

import org.jooq.Identity;
import org.jooq.UniqueKey;
import org.jooq.impl.Internal;
import org.jooq.types.ULong;


/**
 * A class modelling foreign key relationships and constraints of tables of 
 * the <code>bloomdb</code> schema.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.12.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // IDENTITY definitions
    // -------------------------------------------------------------------------

    public static final Identity<FiltertypeRecord, ULong> IDENTITY_FILTERTYPE = Identities0.IDENTITY_FILTERTYPE;

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<FiltertypeRecord> KEY_FILTERTYPE_PRIMARY = UniqueKeys0.KEY_FILTERTYPE_PRIMARY;
    public static final UniqueKey<FiltertypeRecord> KEY_FILTERTYPE_EXPECTEDELEMENTS = UniqueKeys0.KEY_FILTERTYPE_EXPECTEDELEMENTS;

    // -------------------------------------------------------------------------
    // FOREIGN KEY definitions
    // -------------------------------------------------------------------------


    // -------------------------------------------------------------------------
    // [#1459] distribute members to avoid static initialisers > 64kb
    // -------------------------------------------------------------------------

    private static class Identities0 {
        public static Identity<FiltertypeRecord, ULong> IDENTITY_FILTERTYPE = Internal.createIdentity(Filtertype.FILTERTYPE, Filtertype.FILTERTYPE.ID);
    }

    private static class UniqueKeys0 {
        public static final UniqueKey<FiltertypeRecord> KEY_FILTERTYPE_PRIMARY = Internal.createUniqueKey(Filtertype.FILTERTYPE, "KEY_filtertype_PRIMARY", Filtertype.FILTERTYPE.ID);
        public static final UniqueKey<FiltertypeRecord> KEY_FILTERTYPE_EXPECTEDELEMENTS = Internal.createUniqueKey(Filtertype.FILTERTYPE, "KEY_filtertype_expectedElements", Filtertype.FILTERTYPE.EXPECTEDELEMENTS, Filtertype.FILTERTYPE.TARGETFPP, Filtertype.FILTERTYPE.PATTERN);
    }
}
