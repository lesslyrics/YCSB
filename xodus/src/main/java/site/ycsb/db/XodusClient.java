package site.ycsb.db;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import jetbrains.exodus.newLogConcept.transaction.Transaction;
import site.ycsb.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * Client class for the Xodus DB.
 **/
public class XodusClient extends DB {

//  private Environment env;
//  private Store store;

  private static final String DB_PATH = "/home/alinaboshchenko/.myAppData";
  private static final String STORE_NAME = "store";
  private MVCCDataStructure mvccComponent = new MVCCDataStructure();

  @Override
  public void init() {
//    env = Environments.newInstance(DB_PATH);
//    store = env.computeInTransaction(txn -> env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
  }

  // todo smth is wrong with data load

  @Override
  public void cleanup() {
    // todo: complete delete?
    // store.close();
//    env.close();
  }

  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Transaction writeTransaction = mvccComponent.startWriteTransaction();

    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      mvccComponent.put(writeTransaction, StringBinding.stringToEntry(value.getKey()),
          StringBinding.stringToEntry(byteIteratorToString(value.getValue())));
    }
    try {
      mvccComponent.commitTransaction(writeTransaction);
    } catch (ExecutionException | InterruptedException e){
      System.out.println(e);
    }
    return Status.OK;
  }

  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  // todo fix returns
  @Override
  public Status delete(String table, String key) {
    Transaction writeTransaction = mvccComponent.startWriteTransaction();
    mvccComponent.remove(writeTransaction, StringBinding.stringToEntry(key), null);

    return Status.OK;
  }

  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Transaction readTransaction = mvccComponent.startReadTransaction();
    ByteIterable record = mvccComponent.read(readTransaction,
        StringBinding.stringToEntry(key));
    assert record != null;
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // todo ?
    return insert(table, key, values);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    //  we are not interested in the range query benchmarking for now
    // todo not yet implemented
    return Status.OK;
  }
}
