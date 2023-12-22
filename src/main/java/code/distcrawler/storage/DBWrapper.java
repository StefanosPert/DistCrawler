package code.distcrawler.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;

/** (MS1, MS2) A wrapper class which should include:
  * - Set up of Berkeley DB
  * - Saving and retrieving objects including crawled docs and user information
  */
public class DBWrapper {
	
	private static final String CLASS_CATALOG="class_catalog";
	private static final String USER_CATALOG="user_catalog";
	private static final String STATE_CATALOG="state_catalog";
	private static final String PAGE_CATALOG="page_catalog";
	private static final String CHANNEL_CATALOG="channel_catalog";
	private static String envDirectory = null;
	
	private static ConcurrentHashMap<String,Environment> myEnv=new ConcurrentHashMap<String,Environment>();
	private Environment localEnv=null;
	private static EntityStore store;
	
	private StoredClassCatalog catalog=null;
	private Database catalogDB=null;
	private Database userDB=null;
	private Database pageDB=null;
	private Database channelDB=null;
	private Database stateDB=null;
	
	private EntryBinding userKeyBinding;
	private EntryBinding userValBinding;
	private EntryBinding pageKeyBinding;
	private EntryBinding pageValBinding;
	private EntryBinding stateKeyBinding;
	private EntryBinding stateValBinding;
	private EntryBinding channelKeyBinding;
	private EntryBinding channelValBinding;

	public DBWrapper(String workDir) {
		if(workDir!=null && envDirectory==null) {
			envDirectory=workDir;
		}
		try {
			if(workDir!=null) {
				if(myEnv.containsKey(workDir)) {
					localEnv=myEnv.get(workDir);
				} else {
					EnvironmentConfig envConfig= new EnvironmentConfig();
					envConfig.setTransactional(true);
					envConfig.setAllowCreate(true);
					localEnv=new Environment(new File(workDir), envConfig);
					myEnv.put(workDir, localEnv);
				}
			} else {
				localEnv=myEnv.get(envDirectory);
			}
			DatabaseConfig dbConfig=new DatabaseConfig();
			dbConfig.setTransactional(true);
			dbConfig.setAllowCreate(true);
			
			this.catalogDB=localEnv.openDatabase(null, CLASS_CATALOG, dbConfig);
			this.catalog=new StoredClassCatalog(this.catalogDB);
			
			this.userDB=localEnv.openDatabase(null, USER_CATALOG, dbConfig);
			this.userKeyBinding=new SerialBinding(this.catalog,UserKey.class);
			this.userValBinding=new SerialBinding(this.catalog,UserValue.class);
			
			this.stateDB=localEnv.openDatabase(null, STATE_CATALOG, dbConfig);
			this.stateKeyBinding=new SerialBinding(this.catalog,StateKey.class);
			this.stateValBinding=new SerialBinding(this.catalog,StateValue.class);
			
			this.pageDB=localEnv.openDatabase(null, PAGE_CATALOG, dbConfig);
			this.pageKeyBinding=new SerialBinding(this.catalog,PageKey.class);
			this.pageValBinding=new SerialBinding(this.catalog,PageValue.class);
			
			this.channelDB=localEnv.openDatabase(null, CHANNEL_CATALOG, dbConfig);
			this.channelKeyBinding=new SerialBinding(this.catalog,ChannelKey.class);
			this.channelValBinding=new SerialBinding(this.catalog,ChannelValue.class);
			
		}catch(DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	
	public Environment getEnv() {
		return localEnv;
	}
	
	public Database getUserDB() {
		return this.userDB;
	}
	
	public Database getPageDB() {
		return this.pageDB;
	}
	public Database getStateDB() {
		return this.stateDB;
	}
	
	public Database getChannelDB() {
		return this.channelDB;
	}
	
	public EntryBinding getUserKeyBinding() {
		return this.userKeyBinding;
	}
	
	public EntryBinding getValBinding() {
		return this.userValBinding;
	}
	
	public EntryBinding getStateKeyBinding() {
		return this.stateKeyBinding;
	}
	
	public EntryBinding getStateValBinding() {
		return this.stateValBinding;
	}
	
	public EntryBinding getPageKeyBinding() {
		return this.pageKeyBinding;
	}
	
	public EntryBinding getPageValBinding() {
		return this.pageValBinding;
	}
	public EntryBinding getChannelKeyBinding() {
		return this.channelKeyBinding;
	}
	
	public EntryBinding getChannelValBinding() {
		return this.channelValBinding;
	}
	private void removeTransaction(DatabaseEntry key, Database db) {
		Transaction trn=this.getEnv().beginTransaction(null, null);
		trn.setTxnTimeout(4, TimeUnit.SECONDS);
		try {
			db.delete(trn, key);
			trn.commit();
		}catch(Exception e) {
			e.printStackTrace();
			if(trn!=null) {
				trn.abort();
				trn=null;
			}
		}
	}
	public void deleteEntry(PageKey key) {
	               DatabaseEntry delKey=new DatabaseEntry();
	               
	               this.getPageKeyBinding().objectToEntry(key, delKey);
	               
	               Database db=this.getPageDB();
	               this.removeTransaction(delKey, db);
	}
	public void deleteEntry(ChannelKey key) {
		DatabaseEntry delKey=new DatabaseEntry();
		
		this.getChannelKeyBinding().objectToEntry(key, delKey);
		
		Database db=this.getChannelDB();
		this.removeTransaction(delKey, db);
	}
	private void addTranscation(DatabaseEntry inVal, DatabaseEntry inKey, Database db) {
		Transaction trn=this.getEnv().beginTransaction(null, null);
		trn.setTxnTimeout(4, TimeUnit.SECONDS);
		try {
			db.put(trn, inKey, inVal);
			trn.commit();
		}catch(Exception e) {
			e.printStackTrace();
			if(trn!=null) {
				trn.abort();
				trn=null;
			}
		}
		
	}
	public ArrayList<ChannelValue> getChannels(){
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		ArrayList<ChannelValue> result=new ArrayList<ChannelValue>();
		Cursor cursor=null;
		try {
		Database db=this.getChannelDB();
		cursor=db.openCursor(null,null);
		while(cursor.getNext(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS) {
			ChannelValue currVal= (ChannelValue) this.getChannelValBinding().entryToObject(value);
			result.add(currVal);
		}
		}catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(cursor!=null) {
				cursor.close();
			}
		}
		return result;
		
	}
	public void addEntry(UserKey key, UserValue val) {
		DatabaseEntry inVal=new DatabaseEntry();
		DatabaseEntry inKey=new DatabaseEntry();
		
		this.getUserKeyBinding().objectToEntry(key, inKey);
		this.getValBinding().objectToEntry(val, inVal);
		
		Database db=this.getUserDB();
		
		this.addTranscation(inVal, inKey, db);
	}
	
	public void addEntry(StateKey key, StateValue val) {
		DatabaseEntry inVal=new DatabaseEntry();
		DatabaseEntry inKey=new DatabaseEntry();
		
		this.getStateKeyBinding().objectToEntry(key, inKey);
		this.getStateValBinding().objectToEntry(val, inVal);
		
		Database db=this.getStateDB();
		
		this.addTranscation(inVal, inKey, db);
	}
	
	public void addEntry(PageKey key, PageValue val) {
		DatabaseEntry pageVal=new DatabaseEntry();
		DatabaseEntry pageKey=new DatabaseEntry();
		
		this.getPageKeyBinding().objectToEntry(key, pageKey);
		this.getPageValBinding().objectToEntry(val, pageVal);
		
		Database db=this.getPageDB();
		this.addTranscation(pageVal, pageKey, db);
	}
	
	public void addEntry(ChannelKey key, ChannelValue val) {
		DatabaseEntry channelVal=new DatabaseEntry();
		DatabaseEntry channelKey=new DatabaseEntry();
		
		this.getChannelKeyBinding().objectToEntry(key, channelKey);
		this.getChannelValBinding().objectToEntry(val, channelVal);
		
		Database db=this.getChannelDB();
		this.addTranscation(channelVal, channelKey, db);
	}
	private Transaction getTranscation(DatabaseEntry outVal, DatabaseEntry outKey, Database db) throws Exception {
		Transaction trn=this.getEnv().beginTransaction(null, null);
		trn.setTxnTimeout(4, TimeUnit.SECONDS);
		db.get(trn, outKey, outVal, LockMode.DEFAULT);
		trn.commit();
		return trn;
	}
	public UserValue getEntry(UserKey key) {
		DatabaseEntry outVal=new DatabaseEntry();
		DatabaseEntry outKey=new DatabaseEntry();
		UserValue result=null;
		Transaction trn=null;
		
		try {

		this.getUserKeyBinding().objectToEntry(key, outKey);
		Database db=this.getUserDB();
		
		trn=this.getTranscation(outVal, outKey, db);
		
		if(outVal!=null) {
		result=(UserValue) this.getValBinding().entryToObject(outVal);
		}
		}catch(Exception e) {
			//e.printStackTrace();
			if(trn!=null) {
				trn.abort();
				trn=null;
			}
		}
		
		return result;
	}
	
	public StateValue getEntry(StateKey key) {
		DatabaseEntry outVal=new DatabaseEntry();
		DatabaseEntry outKey=new DatabaseEntry();
		StateValue result=null;
		Transaction trn=null;
		
		try {

		this.getStateKeyBinding().objectToEntry(key, outKey);
		Database db=this.getStateDB();
		
		trn=this.getTranscation(outVal, outKey, db);
		if(outVal!=null) {
		result=(StateValue) this.getStateValBinding().entryToObject(outVal);
		}
		}catch(Exception e) {
			//e.printStackTrace();
			if(trn!=null) {
				trn.abort();
				trn=null;
			}
		}
		
		return result;
	}
	
	public PageValue getEntry(PageKey key) {
		DatabaseEntry outVal=new DatabaseEntry();
		DatabaseEntry outKey=new DatabaseEntry();
		PageValue result=null;
		Transaction trn=null;
		try {
			this.getPageKeyBinding().objectToEntry(key, outKey);
			Database db=this.getPageDB();
			
			trn=this.getTranscation(outVal, outKey, db);
			
			if(outVal!=null) {
				result=(PageValue) this.getPageValBinding().entryToObject(outVal);
			}
		}catch(Exception e) {
		//	e.printStackTrace();
			if(trn!=null) {
				trn.abort();
				trn=null;
			}
		}
		return result;
	}
	
	public ChannelValue getEntry(ChannelKey key) {
		DatabaseEntry outVal=new DatabaseEntry();
		DatabaseEntry outKey=new DatabaseEntry();
		ChannelValue result=null;
		Transaction trn=null;
		try {
			this.getChannelKeyBinding().objectToEntry(key, outKey);
			Database db=this.getChannelDB();
			
			trn=this.getTranscation(outVal, outKey, db);
			
			if(outVal!=null) {
				result=(ChannelValue) this.getChannelValBinding().entryToObject(outVal);
			}
		}catch(Exception e) {
		//	e.printStackTrace();
			if(trn!=null) {
				trn.abort();
				trn=null;
			}
		}
		return result;
	}
}
