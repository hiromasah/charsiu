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

public interface DaySpanMap {
	Integer getDaySpan(String aBaseDate, String aTargetDate);

	String addDaySpan(String aBaseDate, Integer aAddDays);

	public interface DaySpanReversibleDictionary {
		Integer getSpan(String aTargetDate);

		String getDate(Integer aTargetSpan);
	}
}