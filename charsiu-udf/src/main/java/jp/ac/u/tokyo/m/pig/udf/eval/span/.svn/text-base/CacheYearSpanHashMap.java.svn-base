package jp.ac.u.tokyo.m.pig.udf.eval.span;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;

import jp.ac.u.tokyo.m.string.StringFormatConstants;

public class CacheYearSpanHashMap implements YearSpanMap {

	// -----------------------------------------------------------------------------------------------------------------

	public static final CacheYearSpanHashMap INSTANCE = new CacheYearSpanHashMap();

	private HashMap<String, YearSpanDictionary> mYearSpanDictionaryMap = new HashMap<String, YearSpanDictionary>();

	// -----------------------------------------------------------------------------------------------------------------

	private CacheYearSpanHashMap() {}

	// -----------------------------------------------------------------------------------------------------------------

	private synchronized YearSpanDictionary getYearSpanDictionary(String aBaseDate) {
		YearSpanDictionary tYearSpanDictionary = mYearSpanDictionaryMap.get(aBaseDate);
		if (tYearSpanDictionary == null) {
			mYearSpanDictionaryMap.put(aBaseDate, tYearSpanDictionary = createYearSpanDictionary(aBaseDate));
		}
		return tYearSpanDictionary;
	}

	private BindingBaseDateYearSpanDictionary createYearSpanDictionary(String aBaseDate) {
		try {
			return new BindingBaseDateYearSpanDictionary(aBaseDate);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Integer getYearSpan(String aBaseDate, String aTargetDate) {
		return getYearSpanDictionary(aBaseDate).getSpan(aTargetDate);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public class BindingBaseDateYearSpanDictionary implements YearSpanDictionary {
		private Calendar mBaseDate;
		private HashMap<String, Integer> mDateMap;

		public BindingBaseDateYearSpanDictionary(String aBaseDate) throws ParseException {
			Calendar tBaseDate = Calendar.getInstance();
			tBaseDate.setTime(StringFormatConstants.FORMAT_DATE.parse(aBaseDate));
			mBaseDate = tBaseDate;
			mDateMap = new HashMap<String, Integer>();
		}

		@Override
		public synchronized Integer getSpan(String aTargetDate) {
			Integer tResultSpan = mDateMap.get(aTargetDate);
			if (tResultSpan == null) {
				try {
					Calendar tTargetDate = Calendar.getInstance();
					tTargetDate.setTime(StringFormatConstants.FORMAT_DATE.parse(aTargetDate));
					// span 計算
					tResultSpan = SpanUtil.calcDifferenceAsYears(mBaseDate, tTargetDate);
					// 登録
					mDateMap.put(aTargetDate, tResultSpan);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			}
			return tResultSpan;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
