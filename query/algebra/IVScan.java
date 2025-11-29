package org.vanilladb.core.query.algebra;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VecRecPair;
import org.vanilladb.core.sql.VecRecPairComp;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IVF_FLATIndex;

import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.distfn.DistanceFn;

public class IVScan implements Scan {

    private TableScan ts;
    private IVF_FLATIndex idx;
    private DistanceFn distFn;
    // private double radius;

    private PriorityQueue<VecRecPairComp> pq;
    private boolean started = false;

    public IVScan(TableScan ts, IVF_FLATIndex idx, DistanceFn distFn) {
        this.ts = ts;
        this.idx = idx;
        this.distFn = distFn;
        // this.radius = radius;
        beforeFirst();
    }

    public void beforeFirst() {
        // System.out.println("IVScan beforeFirst: Initializing scan");

        // 開啟索引掃描
        idx.beforeFirst(distFn);
        // System.out.println("Distance function set: " + distFn);
        // System.out.println("Index scan started");

        // 初始化 max heap
        PriorityQueue<VecRecPairComp> maxPQ = new PriorityQueue<>(20, (a, b) -> b.compareTo(a));
        // System.out.println("Max PriorityQueue initialized with capacity: 20");

        // System.out.println("index :" + idx.getClass().getName());

        int indexCount = 0; // 計算索引記錄數量
        while (idx.next()) {
            //System.out.println("Index record " + indexCount + " scanned");
            VecRecPair vr = idx.getDataVecRecPair();
            VecRecPairComp vrc = new VecRecPairComp(vr, distFn);

            if (maxPQ.size() == 20) {
                // 比較當前元素與堆頂距離大小
                if (vrc.compareTo(maxPQ.peek()) < 0) {
                    VecRecPairComp removed = maxPQ.poll();
                    //System.out.println("Removed farther VecRecPair: " + removed);
                    maxPQ.add(vrc);
                    //System.out.println("Added closer VecRecPair: " + vrc);
                }
            } else {
                maxPQ.add(vrc);
                //System.out.println("Added VecRecPair: " + vrc);
            }

            indexCount++;
        }

        // System.out.println("Index scan completed. Total records processed: " + indexCount);

        // 把 max heap 轉成 min heap
        pq = new PriorityQueue<>(maxPQ);
        // System.out.println("Converted Max PriorityQueue to Min PriorityQueue");
        // System.out.println("Total elements in PriorityQueue: " + pq.size());
        
        // 打印最終的 PriorityQueue 大小
        // System.out.println("Final PriorityQueue size: " + pq.size());

        // Reset the state
        started = false;
        idx.close();
        // System.out.println("Index scan closed");
    }



    @Override
    public boolean next() {
        // System.out.println("IVScan next");
        if (!started) {
            started = true;
            return !pq.isEmpty();
        }
        pq.poll();
        return !pq.isEmpty();
    }

    @Override
    public Constant getVal(String fldName) {
        // System.out.println("IVScan getVal");
        VecRecPairComp vrc = pq.peek();
        if (vrc == null)
            return null;

        ts.moveToRecordId(vrc.getRecordId());
        // System.out.println("Moved to record ID: " + vrc.getRecordId());
        
        return ts.getVal(fldName);
    }

    @Override
    public void close() {
        ts.close();
    }

    @Override
    public boolean hasField(String fldName) {
        // System.out.println("IVScan hasField");
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'hasField'");
    }

}
