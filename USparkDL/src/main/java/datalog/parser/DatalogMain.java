package datalog.parser;

import java.io.IOException;
import it.unical.mat.dlv.parser.ParseException;
import it.unical.mat.dlv.program.Program;
import it.unical.mat.dlv.program.Rule;

public class DatalogMain {

	public static void main(String[] args) throws ParseException, IOException {

		
		
		USparkDLEngine e = new USparkDLEngine();
		
	Program program1 = new Program();
		program1.add(new Rule("a(X, Y) :- b (X, Y)."));
		program1.add(new Rule("c(X, Y) :- d (X, Y)."));
		program1.add(new Rule ("e (X) :- f (X)."));
		program1.add(new Rule ("g(X) :- f(X)."));
		program1.add(new Rule ("h(X) :- g(X)."));
		program1.add(new Rule("b(2, 3)."));
		program1.add(new Rule("b(2, 4)."));
		program1.add(new Rule("d(2, 5)."));
		program1.add(new Rule("d(3, 5)."));
		program1.add(new Rule ("f(3)."));
		program1.add(new Rule ("f(5)."));
	
		
		Program program2 = new Program();
		program2.add(new Rule("a(X) :- not b (X), c(X)."));
		program2.add(new Rule("b(2)."));
		program2.add(new Rule("b(3)."));
		program2.add(new Rule("c(2)."));
		program2.add(new Rule("c(3)."));
		program2.add(new Rule("c(4)."));
		
		Program program3 = new Program();
		program3.add(new Rule("a(X, Y) :- b (X,Y)."));
		program3.add(new Rule("a(X, Y) :- a(X, Z), b (Z,Y)."));
		program3.add(new Rule("b(1, 2)."));
		program3.add(new Rule("b(1, 3)."));
		program3.add(new Rule("b(2, 4)."));
		program3.add(new Rule("b(4, 5)."));
		
		
		Program stratifiedNegation = new Program();
		stratifiedNegation.add(new Rule("a(X) :- b(X)."));
		stratifiedNegation.add(new Rule("a(X) :- a(Y), c(Y,X)."));
		stratifiedNegation.add(new Rule("n(X) :- t(X), not a(X)."));
		stratifiedNegation.add(new Rule("b(2)."));
		stratifiedNegation.add(new Rule("b(4)."));
		stratifiedNegation.add(new Rule("c(2, 5)."));
		stratifiedNegation.add(new Rule("t(3)."));
		stratifiedNegation.add(new Rule("t(2)."));
		
		DatalogProgramProvider positiveNonStratifiedDatalog = new DatalogProgramProvider(program1);
		positiveNonStratifiedDatalog.showDatalogProgram();
		e.semiNaiveEvaluation(positiveNonStratifiedDatalog);

		DatalogProgramProvider simpleNegation = new DatalogProgramProvider(program2);
		simpleNegation.showDatalogProgram();
		e.semiNaiveEvaluation(simpleNegation);
		
		DatalogProgramProvider transitiveClosure = new DatalogProgramProvider(program3);
		transitiveClosure.showDatalogProgram();
		e.semiNaiveEvaluation(transitiveClosure);
		
		
		DatalogProgramProvider stratifiedNegationProgram = new DatalogProgramProvider(stratifiedNegation);
		stratifiedNegationProgram.showDatalogProgram();
		e.semiNaiveEvaluation(stratifiedNegationProgram);
		
		
	}

}
