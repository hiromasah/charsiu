package jp.ac.u.tokyo.m.pig.udf.eval.span;

public interface DaySpanMap {
	Integer getDaySpan(String aBaseDate, String aTargetDate);

	String addDaySpan(String aBaseDate, Integer aAddDays);

	public interface DaySpanReversibleDictionary {
		Integer getSpan(String aTargetDate);

		String getDate(Integer aTargetSpan);
	}
}
