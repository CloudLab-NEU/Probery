package neu.swc.kimble.tables;


public class ReferAttribute {

	private String name;
	private String type;
	private String minValue;
	private String maxValue;
	private String size;
	
	
	public ReferAttribute(){
		
	}
	public ReferAttribute(String name, String type, String minValue,
			String maxValue, String size) {
		super();
		this.name = name;
		this.type = type;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.size = size;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getMinValue() {
		return minValue;
	}
	public void setMinValue(String minValue) {
		this.minValue = minValue;
	}
	public String getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(String maxValue) {
		this.maxValue = maxValue;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	
	public int getPartitionSize(){
		int size = Integer.parseInt(this.size);
		int max_value,min_value;
		
		if(Integer.parseInt(this.type) == 0){
			max_value = Integer.parseInt(this.maxValue);
			min_value = Integer.parseInt(this.minValue);
		}
		else if(Integer.parseInt(this.type) == 1){
			max_value = (int)Double.parseDouble(this.maxValue);
			min_value = (int)Double.parseDouble(this.minValue);
		}
		else{
			max_value = (int)(this.maxValue.charAt(0));
			min_value = (int)(this.minValue.charAt(0));
		}
		
		int temp = max_value - min_value + 1;
		
		if(temp % size == 0)
			return temp/size;
		else
			return temp/size + 1;
	}
	
	public int getAttriRegionNumber(String value){
		int size = Integer.parseInt(this.size);
		int min_value,int_value;
		if(Integer.parseInt(this.type) == 0){
			min_value = Integer.parseInt(this.minValue);
			int_value = Integer.parseInt(value);
		}
		else if(Integer.parseInt(this.type) == 1){
			min_value = (int)Double.parseDouble(this.minValue);
			int_value = (int)Double.parseDouble(value);
		}
		
		else{
			min_value = (int)(this.minValue.charAt(0));
			int_value = (int)(value.charAt(0));
		}
		int temp = int_value - min_value + 1;

		if(temp % size == 0)
			return temp/size;
		else
			return temp/size + 1;
	}
}
