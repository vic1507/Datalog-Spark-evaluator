package datalog.parser;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import it.unical.mat.dlv.parser.Builder;
import it.unical.mat.dlv.parser.Director;
import it.unical.mat.dlv.parser.ParseException;
import it.unical.mat.dlv.program.Program;
import it.unical.mat.dlv.program.Rule;
import it.unical.mat.dlv.program.SimpleProgramBuilder;

public class DatalogProgramProvider {

	private Program program;
	
	public DatalogProgramProvider (Program program)
	{
		this.program = program;
	}
	
	public DatalogProgramProvider (String program) throws UnsupportedEncodingException, ParseException
	{
		this.program = getProgram(program);
	}
	
	/**
	 * Return a Program object starting from a code string.
	 * 
	 * @param code the input code
	 * @return A Program object representing the parsed input code string
	 * @throws UnsupportedEncodingException
	 * @throws ParseException
	 */
	private Program getProgram(String code) throws UnsupportedEncodingException, ParseException {

		Director director = new Director(new ByteArrayInputStream(code.getBytes("UTF-8")));
		Builder builder = new SimpleProgramBuilder();
		director.configureBuilder(builder);
		director.setExistentialTerms(true);
		director.start();

		Program p = (Program) builder.getProductHandler();

		return p;
	}
	
	/**
	 * Return the datalog program handled by the provider
	 * @return the datalog program returned
	 */
	public Program getProgram() {
		return program;
	}
	
	
	/**
	 * Return the EDB part of the program.
	 * 
	 * @param program the datalog program
	 * @return all the facts of the EDB part of the program.
	 */
	public List<Rule> getEDB ()
	{
		return program.getFacts(true);
	}
	
	
	/**
	 * Return the IDB part of a given datalog program.
	 * @param p the given datalog program
	 * @return the idb part of the program
	 */
	public List<Rule> getIDB() {
		List<Rule> IDB = new ArrayList<>();

		program.getRules().forEach(rule -> {
			if (!rule.isFact())
				IDB.add(rule);
		});

		return IDB;
	}
	
	public void showDatalogProgram ()
	{
		System.out.println("**********************SPARK_DL ENGINE**************************");
		System.out.println("EDB start.");
		getEDB().forEach(x-> System.out.println("fact: " + x));
		System.out.println("EDB end.");
		System.out.println("***************************************************************");
		System.out.println("IDB start.");
		getIDB().forEach(x-> System.out.println("rule: " + x));
		System.out.println("IDB end.");
		System.out.println("***************************************************************");
	}
	
}
