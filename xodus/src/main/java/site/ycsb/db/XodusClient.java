package site.ycsb.db;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import site.ycsb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Client class for the Xodus DB.
 **/
public class XodusClient extends DB {

  private Environment env;
  private Store store;

  private static final String DB_PATH = "/home/alinaboshchenko/.myAppData";
  private static final String STORE_NAME = "store";

  @Override
  public void init() {
    env = Environments.newInstance(DB_PATH);
    store = env.computeInTransaction(txn -> env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
  }

  @Override
  public void cleanup() {
    // todo: complete delete?
    // store.close();
    env.close();
  }

  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    env.executeInTransaction(txn -> {
        for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
          store.put(txn, StringBinding.stringToEntry(value.getKey()),
              StringBinding.stringToEntry(byteIteratorToString(value.getValue())));
        }
      });
    return Status.OK;
  }

  private String byteIteratorToString(ByteIterator byteIter) {
    return new String(byteIter.toArray());
  }

  // todo fix returns
  @Override
  public Status delete(String table, String key) {
    env.executeInTransaction(txn -> store.delete(txn, StringBinding.stringToEntry(key)));
    return Status.OK;
  }

  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    env.executeInTransaction(txn -> {
      // todo
        ByteIterable value = store.get(txn, StringBinding.stringToEntry(key));
        assert value != null;
      });
    env.close();
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
