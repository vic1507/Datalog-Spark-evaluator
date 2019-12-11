package datalog.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import it.unical.mat.dlv.program.Literal;
import it.unical.mat.dlv.program.Program;
import it.unical.mat.dlv.program.Rule;
import it.unical.mat.dlv.program.Term;

public class USparkDLEngine {

	/**
	 * Compute the dependency graph for the semi-naive evaluation
	 * 
	 * @param program The input Datalog Program
	 */
	public Map<String, Set<Rule>> computeDependencyGraph(Program program) {
		Set<String> heads = new HashSet<>();
		Set<String> bodies = new HashSet<>();
		program.getRules().forEach(rule -> bodies.addAll(getRuleNames(rule, true)));
		program.getRules().forEach(rule -> heads.addAll(getRuleNames(rule, false)));

		Map<String, Set<Rule>> dependencies = new HashMap<>();
		program.getRules().forEach(rule -> rule.getBody().forEach(predicate -> {
			String name = predicate.getName();
			if (!dependencies.containsKey(name) && !rule.isFact()) {
				dependencies.put(name, new HashSet<Rule>());
				dependencies.get(name).add(rule);
			} else if (!rule.isFact())
				dependencies.get(name).add(rule);
		}));

		return dependencies;
	}

	public List<List<Rule>> wrapStratification(List<Rule> allRules) {
		List<List<Rule>> stratifiedRules = new ArrayList<>();
		stratifiedRules.addAll(computeStratification(allRules));
		return stratifiedRules;
	}

	/**
	 * Strating point (like a main) for the semi-naive evaluation
	 * 
	 * @param provider Provider containing the datalog program.
	 */
	public void semiNaiveEvaluation(DatalogProgramProvider provider)  {

		System.out.println("************EVALUATION START************");
		System.out.println("dependency graph");

		Map<String, Set<Rule>> rules = computeDependencyGraph(provider.getProgram());
		for (String s : rules.keySet())
			rules.get(s).forEach(x -> System.out.println(s + " : " + x));

		System.out.println("***************************************");
		System.out.println("*********STARTING DATABASE EXPANDING**************");

		List<Rule> expandedDatabase = expandDatabase(provider.getIDB(), provider.getEDB(), rules);

		System.out.println("*********FINISH DATABASE EXPANDING**************");

		System.out.println("******************FINAL RESULTS*****************");
		expandedDatabase.forEach(x -> System.out.println(x));
		System.out.println("END USPARKDL.");

		System.out.println();
		System.out.println();
		System.out.println();
		System.out.println();
	}

	/**
	 * Return all the predicates in the head or in the body of the rule.
	 * 
	 * @param rule The input rule
	 * @param body Boolean value that indicates if we want body predicates or head
	 *             predicates
	 * @return all the names of the indicated predicates
	 */
	public Set<String> getRuleNames(Rule rule, boolean body) {
		Set<String> names = new HashSet<>();
		if (body)
			rule.getBody().forEach(atom -> names.add(atom.getName()));
		else
			rule.getHead().forEach(literal -> names.add(literal.getName()));
		return names;
	}

	/**
	 * Incapsulate the semi-naive evaluation
	 * 
	 * @param allRules        all the rules of the datalog program.
	 * @param facts           edb part
	 * @param dependencyGraph dependency graph of the rules
	 * @return
	 * @throws Exception 
	 */
	public List<Rule> expandDatabase(List<Rule> allRules, List<Rule> facts, Map<String, Set<Rule>> dependencyGraph)  {

		System.out.println("PRE STRATI");
		List<List<Rule>> stratifiedRule = wrapStratification(allRules);
		List<Rule> newFacts = new ArrayList<>();
		System.out.println("POST STRATI");
		
		for (int i = 0; i < stratifiedRule.size(); i++) {
			List<Rule> rules = stratifiedRule.get(i);
			newFacts = expandStratification(rules, facts, dependencyGraph);
		}
		return newFacts;
	}

	/**
	 * Core of the semi-naive. Return the result of the evaluation.
	 * 
	 * @param stratifiedRules list of input rules (stratified)
	 * @param facts           edb part
	 * @param dependencyGraph dependency graph of the rules
	 * @return
	 */
	public List<Rule> expandStratification(List<Rule> stratifiedRules, List<Rule> facts,
			Map<String, Set<Rule>> dependencyGraph) {

		if (stratifiedRules.isEmpty() || stratifiedRules == null)
			return Collections.emptyList();

		System.out.println("EXPAND STRATIFICATION PRE WHILE");
		List<Rule> rules = stratifiedRules;
		while (true) {
			System.out.println("EXPAND STRATIFICATION IN WHILE");
			List<Rule> discoveredFacts = new ArrayList<>();
			for (Rule r : rules) {
				System.out.println("EXPAND RULES IN FOR");
				discoveredFacts.addAll(generate(facts, r));
				System.out.println("fatti aggiornati: ");
				discoveredFacts.forEach(x -> System.out.println(x));
				System.out.println("*****************************************************************");
			}
			// generate the new facts until no new facts are discovered
			if (discoveredFacts.isEmpty())
				return facts;

			rules = getDependendRules(rules, discoveredFacts);

			facts.addAll(discoveredFacts);
			System.out.println("*******************************************************************");

		}

	}

	/**
	 * return the rules that are affected by some facts
	 * 
	 * @param rules           all the rules of the datalog program
	 * @param discoveredFacts list of fatcs
	 * @return a list of rule that are affected by the input facts
	 */
	private List<Rule> getDependendRules(List<Rule> rules, List<Rule> discoveredFacts) {

		List<Rule> dependentRules = new ArrayList<>();

		for (Rule fact : discoveredFacts)
			for (Rule rule : rules)
				if (rule.getBody().containsPredicate(fact.getHead().get(0).getName()) && !dependentRules.contains(rule))
					dependentRules.add(rule);

		return dependentRules;
	}

	/**
	 * Return a list of rules that represente the body of a given rule
	 * 
	 * @param rule Given rule
	 * @return Rule's body in a list
	 */
	private List<Literal> getRulesInBody(Rule rule) {
		List<Literal> body = new ArrayList<Literal>();

		for (int i = 0; i < rule.getBody().size(); i++) {
			if (rule.getBody().get(i).isPositive()) // if positive, add in the head
				body.add(0, rule.getBody().get(i));
			else // if negative, add it at the end of the list
				body.add(body.size(), rule.getBody().get(i));
		}
		return body;
	}

	/**
	 * Generate new facts starting from a rule and a list of facts
	 * 
	 * @param facts list of fatcs
	 * @param rule  starting rule
	 * @return list of rule representing the new facts
	 */
	public List<Rule> generate(Iterable<Rule> facts, Rule rule) {

		System.out.println("GENERATE FUNCTION");
		List<Rule> generatedFacts = new ArrayList<>();

		List<Map<String, String>> generatedNewFacts = generateNewFacts(facts, getRulesInBody(rule),
				new HashMap<String, String>());

		for (Map<String, String> factsDataStracture : generatedNewFacts) {
			WrapFact wrappedRule = new WrapFact(rule.getHead().get(0).getName());

			for (int i = 0; i < rule.getHead().get(0).getAttributes().size(); i++)
				wrappedRule.addTerm(factsDataStracture.get(rule.getHead().get(0).getAttributes().get(i).toString()));
			generatedFacts.add(new Rule(wrappedRule.toString()));
		}

		List<Rule> resultingFacts = new ArrayList<>();
		for (Rule r : facts)
			resultingFacts.add(r);
		
		return generatedFacts.stream().filter(discoverFact -> !resultingFacts.contains(discoverFact))
				.collect(Collectors.toList());

	}

	private Literal getNewLiteralFromRule(Map<String, String> bindings, Literal goal) {
		WrapFact wrappedRule = new WrapFact(goal.getName());

		for (int i = 0; i < goal.arity(); i++) {
			if (!bindings.containsKey(goal.getAttributeAt(i).toString()))
				wrappedRule.addTerm(goal.getAttributeAt(i).toString());
			else
				wrappedRule.addTerm(bindings.get(goal.getAttributeAt(i).toString()));
		}

		return new Literal(new Rule(wrappedRule.toString()).getHead().get(0));
	}

	/**
	 * Generate recursively new facts starting from a list of facts and the body of
	 * a rule
	 * 
	 * @param facts    list of facts
	 * @param body     the body of the rule
	 * @param bindings data stracture containing the intermediate values.
	 * @return A list of the data stracture containing the new facts
	 */
	private List<Map<String, String>> generateNewFacts(Iterable<Rule> facts, List<Literal> body,
			Map<String, String> bindings) {

		boolean lastGoal = body.size() == 1;
		Literal currentRule = body.get(0);
		List<Map<String, String>> answers = new ArrayList<>();

		if (currentRule.isPositive()) {
			for (Rule fact : facts) {
				if (fact.getHead().get(0).getName().equals(currentRule.getName())) {
					Map<String, String> newBindings = new HashMap<String, String>(bindings);
					if (unify(currentRule, fact, newBindings)) {
						if (lastGoal)
							answers.add(newBindings);
						else
							answers.addAll(generateNewFacts(facts, body.subList(1, body.size()), newBindings));
					}

				}
			}
		} else {
			if (!bindings.isEmpty()) {
				currentRule = getNewLiteralFromRule(bindings, currentRule);
			}
			for (Rule fact : facts) {
				if (fact.getHead().get(0).getName().equals(currentRule.getName())) {
					Map<String, String> newBindings = new HashMap<String, String>(bindings);
					if (unify(currentRule, fact, newBindings)) {
						return Collections.emptyList();
					}
				}
			}
			if (lastGoal)
				answers.add(bindings);
			else
				answers.addAll(generateNewFacts(facts, body.subList(1, body.size()), bindings));
		}

		return answers;
	}

	/**
	 * Check recursively if two literal unify for the body.
	 * 
	 * @param currentRule first literal to check
	 * @param fact        second literal (usually a fact but not necessary) to check
	 * @param answers     a map containing a key value pair variable / variable's
	 *                    value.
	 * @return
	 */
	private boolean unify(Literal currentRule, Rule fact, Map<String, String> answers) {
		for (int i = 0; i < currentRule.getAttributes().size(); i++) {

			Term termFirstRule = currentRule.getAttributes().get(i);
			String firstTerm = termFirstRule.toString();

			Term termSecondRule = fact.getHead().get(0).getAttributes().get(i);
			String secondTerm = termSecondRule.toString();

			if (termFirstRule.isVariable()) {
				if (!firstTerm.equals(secondTerm)) {
					if (!answers.containsKey(firstTerm))
						answers.put(firstTerm, secondTerm);
					else if (!answers.get(firstTerm).equals(secondTerm))
						return false;
				}
			} else if (termSecondRule.isVariable()) {
				if (!answers.containsKey(secondTerm)) {
					answers.put(secondTerm, firstTerm);
				} else if (!answers.get(secondTerm).equals(firstTerm))
					return false;

			} else if (!firstTerm.equals(secondTerm))
				return false;

		}

		return true;
	}

	
	public static List< List<Rule> > computeStratification(List<Rule> allRules){
        List<List<Rule>> strata = new ArrayList<>(10);

        Map<String, Integer> strats = new HashMap<>();
        for(Rule rule : allRules) {
            String pred = rule.getHead().get(0).getName();
            Integer stratum = strats.get(pred);
            if(stratum == null) {
                stratum = depthFirstSearch(new Literal(rule.getHead().get(0)), allRules, new ArrayList<>(), 0);
                strats.put(pred, stratum);
            }

            while(stratum >= strata.size()) {
                strata.add(new ArrayList<>());
            }
            strata.get(stratum).add(rule);
        }

        strata.add(allRules);
        return strata;
    }
    
    /* The recursive depth-first method that computes the stratification of a set of rules */
    private static int depthFirstSearch(Literal goal, Collection<Rule> graph, List<Literal> visited, int level) {
        String pred = goal.getName();

        // Step (1): Guard against negative recursion
        boolean negated = !goal.isPositive();
        for(int i = visited.size()-1; i >= 0; i--) {
            Literal e = visited.get(i);
            if(e.getName().equals(pred)) {
                if(negated) {
                   System.err.println("Program is not stratified - predicate " + pred + " has a negative recursion");
                }
                return 0;
            }
            if(!e.isPositive()) {
                negated = true;
            }
        }
        visited.add(goal);

        // Step (2): Do the actual depth-first search to compute the strata
        int m = 0;
        for(Rule rule : graph) {
            if(rule.getHead().get(0).getName().equals(pred)) {
                for(Literal expr : rule.getBody()) {
                    int x = depthFirstSearch(expr, graph, visited, level + 1);
                    if(!expr.isPositive())
                        x++;
                    if(x > m) {
                        m = x;
                    }
                }
            }
        }
        visited.remove(visited.size()-1);

        return m;
    }
    
}
