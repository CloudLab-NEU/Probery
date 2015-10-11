package neu.swc.kimble.probabilityModel;

import java.util.ArrayList;

public class DataSpace {
	
	private String name;
	private ArrayList<Block> blockSet;
	
	public DataSpace(String name){
		this.name = name;
		this.blockSet = new ArrayList<Block>();
	}
	
	public String getName(){
		return this.name;
	}
	
	private boolean existingBlock(int blockNumber){
		for(Block attributeRegion:this.blockSet){
			if(attributeRegion.getBlockNumber() == blockNumber)
				return true;
		}
		return false;
	}
	
	public void addBlock(int blockNumber){
		if(!this.existingBlock(blockNumber))
			this.blockSet.add(new Block(blockNumber,this.name));
	}
	
	public void addBlock(Block block){
		this.blockSet.add(block);
	}
	
	public Block getBlock(int blockNumber){
		for(Block block:this.blockSet){
			if(block.getBlockNumber() == blockNumber)
				return block;
		}
		return null;
	}
	
	public ArrayList<Block> getBlockSet(){
		return this.blockSet;
	}
}
