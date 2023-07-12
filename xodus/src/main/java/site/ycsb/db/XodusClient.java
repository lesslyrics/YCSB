package site.ycsb.db;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import jetbrains.exodus.newLogConcept.transaction.Transaction;
import org.json.JSONObject;
import site.ycsb.*;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Client class for the Xodus DB.
 **/
public class XodusClient extends DB {

//  private Environment env;
//  private Store store;

  private MVCCDataStructure mvccComponent = new MVCCDataStructure();
  private PrintStream ps;


  @Override
  public void init() {
    try {
      ps = new PrintStream("stacktrace.log");
    } catch (FileNotFoundException ignored) {
      System.out.println("File not found");
    }
//    env = Environments.newInstance(DB_PATH);
//    store = env.computeInTransaction(txn -> env.openStore(STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn));
  }

  // todo smth is wrong with data load

  @Override
  public void cleanup() {
    // todo: complete delete?
  }



  // TODO: not sure ab this: key = The record key of the record to insert.
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Transaction writeTransaction = mvccComponent.startWriteTransaction();
    mvccComponent.put(writeTransaction, StringBinding.stringToEntry(key),
        StringBinding.stringToEntry(convertToJsonString(values)));
    try {
      mvccComponent.commitTransaction(writeTransaction);
    } catch (ExecutionException | InterruptedException e){
      System.out.println(Arrays.toString(e.getStackTrace()));
      e.printStackTrace(ps);
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
    return insert(table, key, values);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    //  we are not interested in the range query benchmarking for now
    return Status.OK;
  }

  public String convertToJsonString(Map<String, ByteIterator> keyValueMap) {
    JSONObject json = new JSONObject();

    for (Map.Entry<String, ByteIterator> entry : keyValueMap.entrySet()) {
      String key = entry.getKey();
      String value = byteIteratorToString(entry.getValue());
      json.put(key, value);
    }
    return json.toString();
  }

}
