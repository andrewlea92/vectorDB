package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.IVPlan;
// import org.vanilladb.core.query.algebra.IVPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private Plan child;

    public NearestNeighborPlan(Plan p, DistanceFn distFn, Transaction tx) {
        // this.child = new SortPlan(p, distFn, tx);
        var iis = VanillaDb.catalogMgr().getIndexInfo("sift", "i_emb", tx);
        System.out.println("IVF_Flat index plan created...");
        this.child = new IVPlan((TablePlan) p, iis.get(0),distFn, tx);
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        return new NearestNeighborScan(s);
    }

    @Override
    public long blocksAccessed() {
        return child.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public Histogram histogram() {
        return child.histogram();
    }

    @Override
    public long recordsOutput() {
        return child.recordsOutput();
    }
}       
