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
package com.teragrep.pth_06.ast.meta;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.types.UInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Objects;

import static com.teragrep.pth_06.jooq.generated.streamdb.Streamdb.STREAMDB;

/**
 * Finds all STREAMDB.STREAM.ID that match a given condition
 */
public final class StreamIDs {

    private final Logger LOGGER = LoggerFactory.getLogger(StreamIDs.class);
    private final DSLContext ctx;
    private final Condition condition;

    public StreamIDs(final DSLContext ctx, final Condition condition) {
        this.ctx = ctx;
        this.condition = condition;
    }

    public List<Long> streamIdList() {
        final Result<Record1<UInteger>> streamIdResult = ctx
                .select(STREAMDB.STREAM.ID)
                .from(STREAMDB.STREAM)
                .join(STREAMDB.LOG_GROUP)
                .on(STREAMDB.STREAM.GID.eq(STREAMDB.LOG_GROUP.ID))
                .join(STREAMDB.HOST)
                .on(STREAMDB.LOG_GROUP.ID.eq(STREAMDB.HOST.GID))
                .where(condition)
                .fetch();

        LOGGER.info("Stream id results:<\n{}>", streamIdResult);

        final List<Long> streamIdList = streamIdResult
                .getValues(STREAMDB.STREAM.ID, UInteger.class)
                .stream()
                .map(UInteger::longValue)
                .collect(Collectors.toList());

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("fetched <{}> stream id(s)", streamIdList.size());
        }
        return streamIdList;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final StreamIDs streamIDs = (StreamIDs) o;
        return Objects.equals(ctx, streamIDs.ctx) && Objects.equals(condition, streamIDs.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctx, condition);
    }

}
