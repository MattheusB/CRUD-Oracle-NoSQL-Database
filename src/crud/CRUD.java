package crud;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

public class CRUD {

    private final KVStore store;

    public static void main(String args[]) {
        try {
            CRUD example = new CRUD(args);
            example.runExample();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    CRUD(String[] argv) {

        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";
        
        store = KVStoreFactory.getStore
            (new KVStoreConfig(storeName, hostName + ":" + hostPort));
    }

    
    public void addAluno(String matricula, String aluno) {
    	if (store.get(Key.createKey(matricula)) == null) {
    		store.put(Key.createKey(matricula),
            Value.createValue(aluno.getBytes()));
    		System.out.println("Aluno matriculado.");
    	} else {
    		System.out.println("Aluno já está matriculado.");
    	}
    }
    
    public void getAluno(String matricula) {
    	if (store.get(Key.createKey(matricula)) != null) {
    		ValueVersion valueVersion = store.get(Key.createKey(matricula));
    		System.out.println(matricula + " " + new String(valueVersion.getValue().getValue()));
    	} else {
    		System.out.println("Aluno não matriculado.");
    	}
    }
    
    public void updateAluno(String matricula, String novo) {
    	if (store.get(Key.createKey(matricula)) != null) {
    		Value newValue = Value.createValue(novo.getBytes());
    		store.putIfVersion(Key.createKey(matricula), 
    			newValue, store.get(Key.createKey(matricula)).getVersion());
    		System.out.println("Cadastro do aluno atualizado.");
    	} else {
    		System.out.println("Aluno não matriculado.");
    	}
    }
    
    public void deleteAluno(String matricula) {
    	if (store.get(Key.createKey(matricula)) != null) {
    		store.delete(Key.createKey(matricula));
    		System.out.println("Aluno desmatriculado.");
    	} else {
    		System.out.println("Aluno não matriculado.");
    	}
    }
    
    void runExample() {

        store.close();
    }
}
