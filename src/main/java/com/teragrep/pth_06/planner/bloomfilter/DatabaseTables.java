package com.teragrep.pth_06.planner.bloomfilter;

import org.jooq.Table;

import java.util.List;

public interface DatabaseTables {

    List<Table<?>> tables();
}
