package datalog.parser.sparkdl;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import datalog.parser.DatalogProgramProvider;
import datalog.parser.USparkDLEngine;
import it.unical.mat.dlv.program.Program;
import it.unical.mat.dlv.program.Rule;
import scala.Tuple2;
import scala.reflect.ClassTag;

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

		// Program program1 = new Program();
		// program1.add(new Rule("a(X) :- b (X)."));
		// program1.add(new Rule("b(2)."));
		// program1.add(new Rule("b(3)."));

		Program program2 = new Program();
		program2.add(new Rule("a(X) :- b(X)."));
		program2.add(new Rule("a(X):- a(Y), c(Y,X)."));
		program2.add(new Rule("n(X) :- t(X), not a(X)."));
		program2.add(new Rule("b(2)."));
		program2.add(new Rule("b(4)."));
		program2.add(new Rule("c(2,5)."));
		program2.add(new Rule("t(3)."));
		program2.add(new Rule("t(2)."));
		

		// method1(jsc, program1);
		stratifiedMethod(jsc, program2);
		// graphX(jsc);

		// method1(jsc, program1);

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
					.mapToPair(rule -> new Tuple2<String, Rule>(rule.getHead().get(0).getName(), rule));

			// K: b V : a (x) :- b(x); c(x) :- b(x) (tutte le regole che dipendono da x)
			JavaPairRDD<String, Iterable<Rule>> idbStructure = parallelizedIdb.flatMapToPair(rule -> {
				List<Tuple2<String, Rule>> result = new ArrayList<Tuple2<String, Rule>>();
				for (int i = 0; i < rule.getBody().size(); i++) {
					result.add(new Tuple2<String, Rule>(rule.getBody().get(i).getName(), rule));
				}
				return result.iterator();
			}).partitionBy(new HashPartitioner(idb.size())).groupByKey();

			// ottengo b : fatto - regole
			// b: altro fatto - regole ... gruppo per chiave ed ho b ... iterable di fatto,
			// regole
			JavaPairRDD<String, Iterable<Tuple2<Rule, Iterable<Rule>>>> catalog = parallelizedDelta
					.partitionBy(new HashPartitioner(delta1.size())).join(idbStructure).groupByKey();

			// giocata di prestigio. trasformo in modo da avere regola1 --- tutti i fatti
			// che la interessano, poi uso generate e trovo nuovi fatti
			JavaRDD<Rule> newDelta = catalog.flatMapToPair(t -> {

				List<Tuple2<Rule, Rule>> object = new ArrayList<>();
				for (Tuple2<Rule, Iterable<Rule>> tuple : t._2) {
					for (Rule r : tuple._2)
						object.add(new Tuple2<Rule, Rule>(r, tuple._1));
				}
				return object.iterator();
			}).partitionBy(new HashPartitioner(10)).groupByKey().flatMap(t -> {
				USparkDLEngine engine = new USparkDLEngine();
				return engine.generate((Iterable<Rule>) t._2, t._1).iterator();
			});

			delta1 = newDelta.collect();
		}

		for (Rule r : results)
			System.out.println(r);

		jsc.close();
	}

	
	
	public static Tuple2<String,String> getTupleFromString(Rule r)
	{
		return null;
	}
	
	public static void stratifiedMethod(JavaSparkContext jsc, Program program1) {

		DatalogProgramProvider positiveNonStratifiedDatalog = new DatalogProgramProvider(program1);
		List<Rule> edb = positiveNonStratifiedDatalog.getEDB();
		List<Rule> delta1 = edb;
		List<Rule> results = new ArrayList<Rule>();
		USparkDLEngine masterEngine = new USparkDLEngine();
		List<List<Rule>> straties = masterEngine.wrapStratification(positiveNonStratifiedDatalog.getIDB());
		
		for (List<Rule> stratum : straties) {
			while (!delta1.isEmpty()) {
				results.addAll(delta1);

				JavaRDD<Rule> parallelizedEdb = jsc.parallelize(delta1);
				JavaRDD<Rule> parallelizedStratum = jsc.parallelize(stratum);

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
				}).partitionBy(new HashPartitioner(stratum.size())).groupByKey();

				// ottengo b : fatto - regole
				// b: altro fatto - regole ... gruppo per chiave ed ho b ... iterable di fatto,
				// regole
				JavaPairRDD<String, Iterable<Tuple2<Rule, Iterable<Rule>>>> catalog = delta.join(stratumStructure)
						.partitionBy(new HashPartitioner(delta1.size())).groupByKey();

				// giocata di prestigio. trasformo in modo da avere regola1 --- tutti i fatti
				// che la interessano, poi uso generate e trovo nuovi fatti
				JavaRDD<Rule> newDelta = catalog.flatMapToPair(t -> {

					List<Tuple2<Rule, Rule>> object = new ArrayList<>();
					for (Tuple2<Rule, Iterable<Rule>> tuple : t._2) {
						for (Rule r : tuple._2)
							object.add(new Tuple2<Rule, Rule>(r, tuple._1));
					}
					return object.iterator();
				}).partitionBy(new HashPartitioner(10)).groupByKey().flatMap(t -> {
					USparkDLEngine nodeEngine = new USparkDLEngine();
					return nodeEngine.generate((Iterable<Rule>) t._2, t._1).iterator();
				});

				delta1 = newDelta.collect();
			}
			
		}
		
		results.stream().distinct().forEach(System.out::println);

		jsc.close();
	}

}
