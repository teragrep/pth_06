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
package com.teragrep.pth_06.planner;

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

import com.teragrep.pth_06.planner.offset.DatasourceOffset;
import com.teragrep.pth_06.planner.offset.KafkaOffset;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DatasourceOffsetTest {

    @Test
    public void serdeTest() {
        LongOffset longOffset = new LongOffset(0L);
        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        topicPartitionLongMap.put(new TopicPartition("test", 0), 0L);
        KafkaOffset kafkaOffset = new KafkaOffset(topicPartitionLongMap);
        DatasourceOffset datasourceOffset = new DatasourceOffset(longOffset, kafkaOffset);

        String ser = datasourceOffset.json();
        DatasourceOffset deser = new DatasourceOffset(ser);

        Assertions.assertEquals(0L, deser.getArchiveOffset().offset());
        Assertions.assertEquals(1, deser.getKafkaOffset().getOffsetMap().size());
    }

    @Test
    public void kafkaOffsetSerdeTest() {
        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        topicPartitionLongMap.put(new TopicPartition("test", 777), 9999L);
        KafkaOffset kafkaOffset = new KafkaOffset(topicPartitionLongMap);

        String ser = kafkaOffset.json();
        KafkaOffset deser = new KafkaOffset(ser);

        for (Map.Entry<TopicPartition, Long> entry : deser.getOffsetMap().entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long offset = entry.getValue();

            Assertions.assertEquals("test", topicPartition.topic());
            Assertions.assertEquals(777, topicPartition.partition());
            Assertions.assertEquals(9999L, offset);
        }
    }
}
