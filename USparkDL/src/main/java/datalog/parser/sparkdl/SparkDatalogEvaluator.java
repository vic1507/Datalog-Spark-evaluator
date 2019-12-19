package datalog.parser.sparkdl;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import datalog.parser.DatalogProgramProvider;
import datalog.parser.USparkDLEngine;
import it.unical.mat.dlv.program.Program;
import it.unical.mat.dlv.program.Rule;
import scala.Tuple2;

public class SparkDatalogEvaluator {

	public static void main(String args[]) {

		SparkSession.Builder builder = SparkSession.builder().appName("queries");
		builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		SparkConf conf = new SparkConf().setAppName("spark_datalog");
		Class<?>[] classes = new Class[] { Rule.class };
		conf.registerKryoClasses(classes);
		builder.config(conf);
		SparkSession session = builder.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

		Program program1 = new Program();
		program1.add(new Rule("a(X, Y) :- b (X, Y)."));
		program1.add(new Rule("c(X, Y) :- d (X, Y)."));
		program1.add(new Rule("e (X) :- f (X)."));
		program1.add(new Rule("g(X) :- f(X)."));
		program1.add(new Rule("h(X) :- g(X)."));
		program1.add(new Rule("b(2, 3)."));
		program1.add(new Rule("b(2, 4)."));
		program1.add(new Rule("d(2, 5)."));
		program1.add(new Rule("d(3, 5)."));
		program1.add(new Rule("f(3)."));
		program1.add(new Rule("f(5)."));

		Program program2 = new Program();
		program2.add(new Rule("a(X) :- c(X), not b(X)."));
		program2.add(new Rule("b(2)."));
		program2.add(new Rule("b(3)."));
		program2.add(new Rule("c(2)."));
		program2.add(new Rule("c(3)."));
		program2.add(new Rule("c(4)."));

		Program program3 = new Program();
		program3.add(new Rule("a(X, Y) :- b (X, Y)."));
		program3.add(new Rule("a(X, Y) :- a(X, Z), b (Z, Y)."));
		program3.add(new Rule("b(1, 2)."));
		program3.add(new Rule("b(1, 3)."));
		program3.add(new Rule("b(2, 4)."));
		program3.add(new Rule("b(4, 5)."));
		
		Program program22 = new Program();
		program22.add(new Rule("a(X) :- b(X)."));
		program22.add(new Rule("a(X):- a(Y), c(Y,X)."));
		program22.add(new Rule("n(X) :- t(X), not a(X)."));
		program22.add(new Rule("b(2)."));
		program22.add(new Rule("b(4)."));
		program22.add(new Rule("c(2,5)."));
		program22.add(new Rule("t(3)."));
		program22.add(new Rule("t(2)."));

		System.out.println("start test 1");
		method1(jsc, program1);
		System.out.println("end test 1");

		System.out.println("start test 2");
		method1(jsc, program2);
		System.out.println("end test 2");

		System.out.println("start test 3");
		stratifiedMethod(jsc, program3);
		System.out.println("end test 3");

		System.out.println("start test 4");
		stratifiedMethod(jsc, program22);
		System.out.println("end test 4");

		jsc.close();

	}

	public static void method1(JavaSparkContext jsc, Program program1) {

		DatalogProgramProvider positiveNonStratifiedDatalog = new DatalogProgramProvider(program1);
		List<Rule> edb = positiveNonStratifiedDatalog.getEDB();
		List<Rule> idb = positiveNonStratifiedDatalog.getIDB();

		List<Rule> delta1 = edb;
		List<Rule> results = new ArrayList<Rule>();

		while (!delta1.isEmpty()) {
			results.addAll(delta1);

			JavaRDD<Rule> parallelizedEdb = jsc.parallelize(delta1);
			JavaRDD<Rule> parallelizedIdb = jsc.parallelize(idb);

			// K : b V : b (1, 2)
			// K: c V : c (1, 2) ecc
			JavaPairRDD<String, Rule> parallelizedDelta = parallelizedEdb
					.mapToPair(rule -> new Tuple2<String, Rule>(rule.getHead().get(0).getName(), rule))
					.partitionBy(new HashPartitioner(delta1.size()));

			// K: b V : a (x) :- b(x); c(x) :- b(x) (tutte le regole che dipendono da x)
			JavaPairRDD<String, Iterable<Rule>> idbStructure = parallelizedIdb.flatMapToPair(rule -> {
				List<Tuple2<String, Rule>> result = new ArrayList<Tuple2<String, Rule>>();
				for (int i = 0; i < rule.getBody().size(); i++) {
					result.add(new Tuple2<String, Rule>(rule.getBody().get(i).getName(), rule));
				}
				return result.iterator();
			}).partitionBy(new HashPartitioner(idb.size())).groupByKey().partitionBy(new HashPartitioner(idb.size()));

			// ottengo b : fatto - regole
			// b: altro fatto - regole ... gruppo per chiave ed ho b ... iterable di fatto,
			// regole
			JavaPairRDD<String, Iterable<Tuple2<Rule, Iterable<Rule>>>> catalog = parallelizedDelta.join(idbStructure)
					.groupByKey();

			// giocata di prestigio. trasformo in modo da avere regola1 --- tutti i fatti
			// che la interessano, poi uso generate e trovo nuovi fatti
			JavaPairRDD<String, Iterable<Rule>> newDelta2 = catalog.flatMapToPair(t -> {

				List<Tuple2<String, Rule>> object = new ArrayList<>();
				for (Tuple2<Rule, Iterable<Rule>> tuple : t._2) {
					for (Rule r : tuple._2)
						object.add(new Tuple2<String, Rule>(r.toString(), tuple._1));
				}
				return object.iterator();
			}).partitionBy(new HashPartitioner(10)).groupByKey();

			JavaRDD<Rule> newDelta1 = newDelta2.flatMap(t -> {
				USparkDLEngine engine = new USparkDLEngine();
				return engine.generate(t._2, new Rule(t._1)).iterator();
			});

			// edb.addAll(newDelta1.collect());
			delta1 = newDelta1.collect();

			// USparkDLEngine engine = new USparkDLEngine();
			// idb = engine.getDependendRules(idb, delta1);
		}

		for (Rule r : results)
			System.out.println(r);

		// jsc.close();
	}

	public static void stratifiedMethod(JavaSparkContext jsc, Program program1) {

		long startTime = System.nanoTime();
		DatalogProgramProvider positiveNonStratifiedDatalog = new DatalogProgramProvider(program1);
		List<Rule> edb = positiveNonStratifiedDatalog.getEDB();
		List<Rule> delta1 = edb;
		List<Rule> results = new ArrayList<Rule>();
		USparkDLEngine masterEngine = new USparkDLEngine();
		List<List<Rule>> straties = masterEngine.wrapStratification(positiveNonStratifiedDatalog.getIDB());

		for (int strat = 0; strat < straties.size(); strat++) {
			List<Rule> rules = straties.get(strat);
			while (!delta1.isEmpty()) {
				results.addAll(delta1);

				JavaRDD<Rule> parallelizedEdb = jsc.parallelize(delta1);
				JavaRDD<Rule> parallelizedStratum = jsc.parallelize(rules);

				// K : b V : b (1, 2)
				// K: c V : c (1, 2) ecc
				JavaPairRDD<String, Rule> delta = parallelizedEdb
						.mapToPair(rule -> new Tuple2<String, Rule>(rule.getHead().get(0).getName(), rule));

				// K: b V : a (x) :- b(x); c(x) :- b(x) (tutte le regole che dipendono da x)
				JavaPairRDD<String, Iterable<Rule>> stratumStructure = parallelizedStratum.flatMapToPair(rule -> {
					List<Tuple2<String, Rule>> result = new ArrayList<Tuple2<String, Rule>>();
					for (int i = 0; i < rule.getBody().size(); i++) {
						result.add(new Tuple2<String, Rule>(rule.getBody().get(i).getName(), rule));
					}
					return result.iterator();
				}).partitionBy(new HashPartitioner(straties.get(strat).size())).groupByKey();

				// ottengo b : fatto - regole
				// b: altro fatto - regole ... gruppo per chiave ed ho b ... iterable di fatto,
				// regole
				JavaPairRDD<String, Iterable<Tuple2<Rule, Iterable<Rule>>>> catalog = delta.join(stratumStructure)
						.partitionBy(new HashPartitioner(delta1.size())).groupByKey();

				// giocata di prestigio. trasformo in modo da avere regola1 --- tutti i fatti
				// che la interessano, poi uso generate e trovo nuovi fatti
				JavaRDD<Rule> newDelta = catalog.flatMapToPair(t -> {

					List<Tuple2<String, Rule>> object = new ArrayList<>();
					for (Tuple2<Rule, Iterable<Rule>> tuple : t._2) {
						for (Rule r : tuple._2)
							object.add(new Tuple2<String, Rule>(r.toString(), tuple._1));
					}
					return object.iterator();
				}).groupByKey().flatMap(t -> {
					USparkDLEngine nodeEngine = new USparkDLEngine();
					return nodeEngine.generate(t._2, new Rule(t._1)).iterator();
				});

				delta1 = newDelta.collect();
				
				//rules = masterEngine.getDependendRules(straties.get(strat), delta1);
			}

			delta1 = results;

		}

		long end1 = System.nanoTime();
		results.stream().distinct().forEach(System.out::println);
		long end = System.nanoTime();
		System.out.println(end1 - startTime);
		System.out.println(end - startTime);
		// jsc.close();
	}

}
