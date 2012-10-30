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
			tBaseDate.setTime(StringFormatConstants.FORMAT_DATE.get().parse(aBaseDate));
			mBaseDate = tBaseDate;
			mDateMap = new HashMap<String, Integer>();
		}

		@Override
		public synchronized Integer getSpan(String aTargetDate) {
			Integer tResultSpan = mDateMap.get(aTargetDate);
			if (tResultSpan == null) {
				try {
					Calendar tTargetDate = Calendar.getInstance();
					tTargetDate.setTime(StringFormatConstants.FORMAT_DATE.get().parse(aTargetDate));
					// calculate span | span 計算
					tResultSpan = SpanUtil.calcDifferenceAsYears(mBaseDate, tTargetDate);
					// registration | 登録
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
