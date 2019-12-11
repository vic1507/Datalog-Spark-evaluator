package datalog.parser;

import java.util.ArrayList;
import java.util.List;

public class WrapFact {

	
	private String head;
	
	private List<String> termsOfHead;

	public WrapFact (String head)
	{
		this.head = head;
		this.termsOfHead = new ArrayList<>();
	}
	
	public boolean addTerm (String term)
	{
		return this.termsOfHead.add(term);
	}
	
	@Override
	public String toString() {
		String newRule = "";
		String concat = newRule.concat(head + " (");
		for (String s : termsOfHead)
			concat = concat.concat(s + ", ");
		
		String substring = concat.substring(0, concat.lastIndexOf(","));
		substring = substring.concat(").");
		return substring;
	}
	
	

}
