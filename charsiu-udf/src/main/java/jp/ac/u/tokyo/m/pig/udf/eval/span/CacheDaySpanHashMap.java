/*
 * Copyright 2012 Hiromasa Horiguchi ( The University of Tokyo )
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.ac.u.tokyo.m.pig.udf.eval.span;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;

import jp.ac.u.tokyo.m.string.StringFormatConstants;

public class CacheDaySpanHashMap implements DaySpanMap {

	// -----------------------------------------------------------------------------------------------------------------

	public static final CacheDaySpanHashMap INSTANCE = new CacheDaySpanHashMap();

	HashMap<String, DaySpanReversibleDictionary> mDaySpanDictionaryMap = new HashMap<String, DaySpanReversibleDictionary>();

	// -----------------------------------------------------------------------------------------------------------------

	private CacheDaySpanHashMap() {}

	// -----------------------------------------------------------------------------------------------------------------

	private synchronized DaySpanReversibleDictionary getDaySpanReversibleDictionary(String aBaseDate) {
		DaySpanReversibleDictionary tDaySpanReversibleDictionary = mDaySpanDictionaryMap.get(aBaseDate);
		if (tDaySpanReversibleDictionary == null) {
			mDaySpanDictionaryMap.put(aBaseDate, tDaySpanReversibleDictionary = createDaySpanReversibleDictionary(aBaseDate));
		}
		return tDaySpanReversibleDictionary;
	}

	private DaySpanReversibleDictionary createDaySpanReversibleDictionary(String aBaseDate) {
		try {
			return new DaySpanBidirectionalHashMapDictionary(aBaseDate);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public String addDaySpan(String aBaseDate, Integer aAddDays) {
		return getDaySpanReversibleDictionary(aBaseDate).getDate(aAddDays);
	}

	@Override
	public Integer getDaySpan(String aBaseDate, String aTargetDate) {
		return getDaySpanReversibleDictionary(aBaseDate).getSpan(aTargetDate);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public class DaySpanBidirectionalHashMapDictionary implements DaySpanReversibleDictionary {
		private Calendar mBaseDate;
		private HashMap<String, Integer> mDateMapStringToInteger;
		private HashMap<Integer, String> mDateMapIntegerToString;

		public DaySpanBidirectionalHashMapDictionary(String aBaseDate) throws ParseException {
			Calendar tBaseDate = Calendar.getInstance();
			tBaseDate.setTime(StringFormatConstants.FORMAT_DATE.get().parse(aBaseDate));
			mBaseDate = tBaseDate;
			mDateMapStringToInteger = new HashMap<String, Integer>();
			mDateMapIntegerToString = new HashMap<Integer, String>();
		}

		private void put(String aTargetDate, Integer aTargetSpan) {
			// Bidirectional registration | 双方向登録
			mDateMapStringToInteger.put(aTargetDate, aTargetSpan);
			mDateMapIntegerToString.put(aTargetSpan, aTargetDate);
		}

		@Override
		public synchronized Integer getSpan(String aTargetDate) {
			Integer tResultSpan = mDateMapStringToInteger.get(aTargetDate);
			if (tResultSpan == null) {
				try {
					Calendar tTargetDate = Calendar.getInstance();
					tTargetDate.setTime(StringFormatConstants.FORMAT_DATE.get().parse(aTargetDate));
					// calculate span | span 計算
					tResultSpan = SpanUtil.calcDifferenceAsDays(mBaseDate, tTargetDate);
					// Bidirectional registration | 双方向登録
					put(aTargetDate, tResultSpan);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			}
			return tResultSpan;
		}

		@Override
		public synchronized String getDate(Integer aTargetSpan) {
			String tResultDate = mDateMapIntegerToString.get(aTargetSpan);
			if (tResultDate == null) {
				Calendar tBaseDate = (Calendar) mBaseDate.clone();
				SpanUtil.addDays(tBaseDate, aTargetSpan);
				put(tResultDate = StringFormatConstants.FORMAT_DATE.get().format(tBaseDate.getTime())
						, aTargetSpan);
			}
			return tResultDate;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
