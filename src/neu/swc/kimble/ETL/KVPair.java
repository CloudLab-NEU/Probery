package neu.swc.kimble.ETL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

public class KVPair<K,V> implements Iterable<K>, Serializable{

	private static final long serialVersionUID = -8860500222305092712L;
	private ArrayList<K> attribute;
	private ArrayList<V> otherAttribute;
	private int index;
	
	public KVPair(){
		this.attribute = new ArrayList<K>();
		this.otherAttribute = new ArrayList<V>();
	}
	
	public void put(K key, V value){
		this.attribute.add(key);
		this.otherAttribute.add(value);
	}
	
	public V get(K key){
		for(int i=0; i<this.attribute.size(); i++){
			if(this.attribute.get(i).equals(key))
				return this.otherAttribute.get(i);
		}
		return null;
	}
	
	public V getCorrespondingValue(){
		return this.otherAttribute.get(index-1);
	}
	
	public TreeMap<K,V> getTreeMap(){
		TreeMap<K,V> treeMap = new TreeMap<K,V>();
		for(int i=0; i<this.attribute.size();i++)
			treeMap.put(this.attribute.get(i), this.otherAttribute.get(i));
		return treeMap;
	}
	
	public boolean deleteAll(){
		this.attribute.clear();
		this.otherAttribute.clear();
		return true;
	}
	
	@Override
	public Iterator<K> iterator() {
		// TODO Auto-generated method stub
		return new MyIterator();
	}
	
	class MyIterator implements Iterator<K>{

		
		public MyIterator(){
			index = 0;
		}
		
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return index != attribute.size();
		}

		@Override
		public K next() {
			// TODO Auto-generated method stub
			return attribute.get(index++);
		}
		
	}
}
