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
package com.teragrep.pth_06.ast.analyze;

import com.teragrep.pth_06.ast.Expression;
import com.teragrep.pth_06.ast.transform.WithDefaultValues;
import com.teragrep.pth_06.config.Config;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.conf.ThrowExceptions;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ScanRanges {

    private final Logger LOGGER = LoggerFactory.getLogger(ScanRanges.class);

    private final Config config;
    private final Expression root;
    private final List<ScanRange> scanRanges;

    public ScanRanges(final Config config) {
        this(config, new WithDefaultValues(config));
    }

    public ScanRanges(Config config, final WithDefaultValues withDefaultValues) {
        this(config, withDefaultValues.transformed());
    }

    public ScanRanges(Config config, final Expression root) {
        this(config, root, new ArrayList<>());
    }

    private ScanRanges(Config config, final Expression root, final List<ScanRange> scanRanges) {
        this.config = config;
        this.root = root;
        this.scanRanges = scanRanges;
    }

    public List<ScanRange> rangeList() {
        final String userName = config.archiveConfig.dbUsername;
        final String password = config.archiveConfig.dbPassword;
        final String url = config.archiveConfig.dbUrl;
        final String journaldbName = config.archiveConfig.dbJournalDbName;
        final String streamdbName = config.archiveConfig.dbStreamDbName;
        final String bloomdbName = config.archiveConfig.bloomDbName;
        final boolean hideDatabaseExceptions = config.archiveConfig.hideDatabaseExceptions;
        Settings settings = new Settings()
                .withRenderMapping(new RenderMapping().withSchemata(new MappedSchema().withInput("streamdb").withOutput(streamdbName), new MappedSchema().withInput("journaldb").withOutput(journaldbName), new MappedSchema().withInput("bloomdb").withOutput(bloomdbName)));
        final Connection connection;
        try {
            connection = DriverManager.getConnection(url, userName, password);
        }
        catch (final SQLException e) {
            throw new RuntimeException("Error getting connection: " + e.getMessage());
        }
        if (hideDatabaseExceptions) {
            settings = settings.withThrowExceptions(ThrowExceptions.THROW_NONE);
            LOGGER.warn("SQL Exceptions set to THROW_NONE");
        }
        final DSLContext ctx = DSL.using(connection, SQLDialect.MYSQL, settings);
        if (scanRanges.isEmpty()) {
            findScanRanges(ctx, root);
        }
        return scanRanges;
    }

    private void findScanRanges(final DSLContext ctx, final Expression expression) {
        if (expression.isLogical()) {
            final List<Expression> children = expression.asLogical().children();
            for (final Expression child : children) {
                if (child.isLogical()) {
                    findScanRanges(ctx, child);
                }
            }
            ScanGroupExpression scanGroupExpression = new ScanGroupExpression(ctx, expression.asLogical());
            scanRanges.addAll(scanGroupExpression.value());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        ScanRanges that = (ScanRanges) o;
        return Objects.equals(config, that.config) && Objects.equals(root, that.root)
                && Objects.equals(scanRanges, that.scanRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, root, scanRanges);
    }
}
