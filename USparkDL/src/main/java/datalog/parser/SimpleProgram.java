package datalog.parser;

import it.unical.mat.dlv.program.Literal;
import it.unical.mat.dlv.program.Program;
import it.unical.mat.dlv.program.Rule;
import it.unical.mat.wrapper.DLVInputProgram;
import it.unical.mat.wrapper.DLVInputProgramImpl;
import it.unical.mat.wrapper.DLVInvocation;
import it.unical.mat.wrapper.DLVWrapper;
import it.unical.mat.wrapper.Model;
import it.unical.mat.wrapper.ModelHandler;
import it.unical.mat.wrapper.ModelResult;
import it.unical.mat.wrapper.Predicate;

public class SimpleProgram {

	/* This is a typical example for DLV wrapper invocation */
	public void simpleInvocation() {

		/* PREPARE INPUT */

		/*
		 * prepare a DLVInputProgram object using external files, simple text or/and
		 * Program object
		 */
		DLVInputProgram inputProgram = new DLVInputProgramImpl();
		/* inputProgram can include one or more external file containing DLV code */
		// inputProgram.addFile(" put here a path file ");

		/* inputProgram can include simple text too */
		// inputProgram.addText("");

		/* inputProgram can include Program objects */
		Program program = new Program();
		program.add(new Rule("a(X, Y) :- b (X, Y)."));
		program.add(new Rule("a(X, Y) :- a (X, Z), a (Z, Y)."));
		program.add(new Rule("b(1, 2)."));
		program.add(new Rule("b(2, 3)."));
		
		inputProgram.includeProgram(program);
		/* PREPARE AND RUN INVOCATION */
		DLVInvocation invocation = DLVWrapper.getInstance()
				.createInvocation("src/main/java/datalog/parser/dlv.mingw-odbc.exe");

		/* can specify the execution option */
		try {
			invocation.addOption("-nofacts");

			invocation.setInputProgram(inputProgram);
			/* PREPARE RESULT HENDLER */
			ModelHandler modelHandler = new ModelHandler() {
				public void handleResult(DLVInvocation observed, ModelResult result) {
					/* this part of code is called for every model computed from invocation */
					/* scroll a model result */
					((Model) result).beforeFirst();
					while (((Model) result).hasMorePredicates()) {
						Predicate predicate = ((Model) result).nextPredicate();
						predicate.beforeFirst();
						while (predicate.hasMoreLiterals()) {
							Literal literal = predicate.nextLiteral();

							System.out.println(literal);
						}
					}
				}
			};

			/* subscribe the henler at invocation */
			invocation.subscribe(modelHandler);

			/* start execution */
			invocation.run();

			/*
			 * the invocation run the execution in a thread, can wait the end of execution
			 * using this function
			 */
			invocation.waitUntilExecutionFinishes();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SimpleProgram sp = new SimpleProgram();
		sp.simpleInvocation();
	}

}
