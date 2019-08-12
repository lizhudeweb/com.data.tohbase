package test.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

public class HBaseScanTest2 {
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void scanTest() throws IOException, ParseException {
//        String call = "17078388295";
        String call = "13407209608";
        String startPoint = "2017-01-01";
        String stopPoint = "2017-03-01";

        HTable hTable = new HTable(conf, "ns_ct:calllog");
        Scan scan = new Scan();
        ScanRowkeyUtil scanRowkeyUtil = new ScanRowkeyUtil (call, startPoint, stopPoint);
        while (scanRowkeyUtil.hasNext()) {
            String[] rowKeys = scanRowkeyUtil.next();
            scan.setStartRow(Bytes.toBytes(rowKeys[0]));
            scan.setStopRow(Bytes.toBytes(rowKeys[1]));

            System.out.println("rowKeys范围：" + rowKeys[0] + "====" + rowKeys[1]);
            System.out.println("时间范围" + rowKeys[0].substring(15, 21) + "---" + rowKeys[1].substring(15, 21));

            ResultScanner resultScanner = hTable.getScanner(scan);
            //每一个rowkey对应一个result
            for (Result result : resultScanner) {
                //每一个rowkey里面包含多个cell
                Cell[] cells = result.rawCells();
                StringBuilder sb = new StringBuilder();
                sb.append(Bytes.toString(result.getRow())).append(",");
                System.out.println("--------------------");
                for (Cell c : cells) {
                    sb.append(Bytes.toString(CellUtil.cloneValue(c))).append(",");
                    System.out.println(Bytes.toString(CellUtil.cloneRow(c))
                            + ","
                            + Bytes.toString(CellUtil.cloneFamily(c))
                            + ":"
                            + Bytes.toString(CellUtil.cloneQualifier(c))
                            + ","
                            + Bytes.toString(CellUtil.cloneValue(c)));
                }
                System.out.println(sb.toString());
            }
        }
    }
}
