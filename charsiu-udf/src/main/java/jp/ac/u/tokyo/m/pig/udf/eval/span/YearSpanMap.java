package jp.ac.u.tokyo.m.pig.udf.eval.span;

public interface YearSpanMap {
	Integer getYearSpan(String aBaseDate, String aTargetDate);

	public interface YearSpanDictionary {
		Integer getSpan(String aTargetDate);
	}
}
