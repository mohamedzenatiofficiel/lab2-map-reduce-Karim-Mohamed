package com.opstty;

import org.apache.hadoop.util.ProgramDriver;

import com.opstty.job.ex1;
import com.opstty.job.ex4;
import com.opstty.job.ex6_2;
import com.opstty.job.ex6_1;
import com.opstty.job.ex2;
import com.opstty.job.ex3;
import com.opstty.job.ex5;
import com.opstty.job.WordCount;

public class AppDriver {
	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver programDriver = new ProgramDriver();

		try {
			programDriver.addClass("wordcount", WordCount.class,
					"Un map/reduce permettant de compter le nombre de mot");

			programDriver.addClass("ex1", ex1.class,
					"MapReduce job that displays the list of distinct containing trees in this dataset");

			programDriver.addClass("ex2", ex2.class,
					"MapReduce job that displays the list of different species trees in this dataset");

			programDriver.addClass("ex3", ex3.class,
					"MapReduce job that calculates the number of trees of each kinds");

			programDriver.addClass("ex4", ex4.class,
					"MapReduce job that calculates the height of the tallest tree of each kind");

			programDriver.addClass("ex5", ex5.class,
					"MapReduce job that sort the trees height from smallest to largest");

			programDriver.addClass("ex6_1", ex6_1.class,
					"Mapper job that displays the district where the oldest tree is.");

			programDriver.addClass("ex6_2", ex6_2.class,
					"Reducer job that displays the district where the oldest tree is.");

			exitCode = programDriver.run(argv);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}

		System.exit(exitCode);
	}
}
