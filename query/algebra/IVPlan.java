package org.vanilladb.core.query.algebra;

import java.util.HashMap;
import java.util.Map;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Kmeans;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorConstantRange;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.IVF_FLATIndex;

public class IVPlan implements Plan {
    private TablePlan child;
    private IndexInfo indexInfo;
    private DistanceFn distFn;
    private Transaction tx;
    private double radius;//有點問題
    public static final int NUM_CLUSTERS;

    static {
         NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(
                IVPlan.class.getName() + ".NUM_CLUSTERS", 200);
    }

    public IVPlan(TablePlan childPlan, IndexInfo indexInfo, DistanceFn distFn, Transaction tx ) {
        this.child = childPlan;
        this.indexInfo = indexInfo;
        this.distFn = distFn;
        //this.radius = radius;
        this.tx = tx;
        //System.out.println("NUM_CLUSTERS: " + NUM_CLUSTERS);
    }

    @Override
    // public Scan open() {
    //     System.out.println("IVPlan open");
    //     TableScan ts = (TableScan) child.open();
    //     IVF_FLATIndex index = indexInfo.openIVF(tx,(int) (0.1*NUM_CLUSTERS));
    //     // return new IVScan(ts, index, distFn, radius);
    //     return new IVScan(ts, index, distFn);
    // }
    public Scan open() {
        //System.out.println("IVPlan open");

        // 打印子計劃信息
        //System.out.println("Opening child plan...");
        TableScan ts = (TableScan) child.open();
        //System.out.println("Child plan opened successfully.");

        // 打印 IndexInfo 的信息
        //System.out.println("Using IndexInfo: " + indexInfo);
        //System.out.println("Transaction: " + tx);
        //System.out.println("Number of clusters for IVF: " + (int) (0.1 * NUM_CLUSTERS));

        // 開啟 IVF Index 並打印相關資訊
        IVF_FLATIndex index = (IVF_FLATIndex)indexInfo.open(tx);
        //System.out.println("IVF Index opened successfully: " + index);

        // 打印距離函數
        //System.out.println("Distance function used: " + distFn);

        // 返回新的 IVScan 並打印相關資訊
        IVScan ivScan = new IVScan(ts, index, distFn);
        //System.out.println("IVScan created successfully: " + ivScan);

        return ivScan;
    }


    @Override
    public long blocksAccessed() {
        //System.out.println("IVPlan blocksAccessed");
        // Index index = indexInfo.open(tx);
        // return child.blocksAccessed() + index.blocksAccessed();
        return child.blocksAccessed();
    }


    @Override
    public long recordsOutput() {
        //System.out.println("IVPlan recordsOutput");
        Histogram hist = SelectPlan.constantRangeHistogram(
            child.histogram(),
            createVectorConstantRanges()
        );
        return (int) hist.recordsOutput();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    private Map<String, ConstantRange> createVectorConstantRanges() {
        //System.out.println("IVPlan createVectorConstantRanges");
        Map<String, ConstantRange> cRanges = new HashMap<>();
        // 確保 VectorConstantRange 是 ConstantRange 的子類別
        cRanges.put("vector_key", new VectorConstantRange(distFn, radius));
        System.out.println("radius: "+radius);
        return cRanges;
    }
    //     private Map<String, ConstantRange> createVectorConstantRanges() {
    //     System.out.println("IVPlan createVectorConstantRanges");
    //     Map<String, ConstantRange> cRanges = new HashMap<>();
    //     VectorConstant 
    //     // int k = QueryContext.getK();
    //     cRanges.put(indexInfo.fieldNames().get(0), new VectorConstantRange(queryVector, distFn, (int) (NUM_CLUSTERS*0.1)));
    //     // System.out.println("k: " + k + ", queryVector: " + queryVector);
    //     return cRanges;
    // }

    @Override
    public Histogram histogram() {
        //System.out.println("IVPlan histogram");
        // TODO Auto-generated method stub
        try{
            return SelectPlan.constantRangeHistogram(
            child.histogram(),
            createVectorConstantRanges()
            );
        }catch (Exception e){
            throw new UnsupportedOperationException("Unimplemented method 'histogram'");
        }   
    }
}
