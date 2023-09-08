package com.example.worker.Indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseIndex {
    private static DatabaseIndex instance;
    public static DatabaseIndex getInstance(){
        if(instance == null)
            instance = new DatabaseIndex();
        return instance;
    }

    ConcurrentHashMap<String , CollectionIndex> map = new ConcurrentHashMap<>();

    public void initIndex(String Database){
        CollectionIndex index = new CollectionIndex();
        map.put(Database,index);
    }

    public boolean addCollection(String Database, String Collection, String []Properties){
        if(!map.containsKey(Database))
            return false;

        return map.get(Database).initIndexForCollection(Collection, Properties);
    }

    public boolean isIndexed(String Database,String Collection){
        if(!map.containsKey(Database))
            return false;

        return map.get(Database).isIndexed(Collection);
    }

    public boolean isIndexed(String Database){
        return map.containsKey(Database);
    }

    public void indexProperties(String Database, String Collection, String Property, String value, int idx){
        if(!map.containsKey(Database))
            return;

        map.get(Database).indexProperties(Collection,Property,value,idx);
    }

    public HashMap<String, List<Integer>> getMap(String Database, String Collection, String Property) {
        return map.get(Database).getMap(Collection,Property);
    }

    public void deleteByIdx(String Database,String Collection,int idx){
        if(!map.containsKey(Database))
            return;

        map.get(Database).deleteByIdx(Collection,idx);
    }

    public void addToValue(String Database, String Collection, String Property, String value, int idx){
        if(!map.containsKey(Database))
            return;

        map.get(Database).addToValue(Collection,Property,value,idx);
    }

    public void DropCollection(String Database, String Collection){
        if(!map.containsKey(Database))
            return;

        map.get(Database).dropCollection(Collection);
    }

    public void DropDatabase(String Database){
        map.remove(Database);
    }

    public List<String> getCollections(String Database){
        if(!map.containsKey(Database))
            return new ArrayList<>();

        return map.get(Database).getCollections();
    }

    public List<String> getDatabases(){
        return new ArrayList<>(map.keySet());
    }
}

