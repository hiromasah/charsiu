package jp.ac.u.tokyo.m.ini;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import jp.ac.u.tokyo.m.ini.Ini.Section;
import junit.framework.Assert;

import org.junit.Test;

public class IniTest {

	// -----------------------------------------------------------------------------------------------------------------

	private Section createSection(String aSectionName, String... aMemberKeyValues) {
		LinkedHashMap<String, String> tMembers = new LinkedHashMap<String, String>();
		int tLength = aMemberKeyValues.length;
		for (int tIndex = 0; tIndex < tLength; tIndex += 2) {
			tMembers.put(aMemberKeyValues[tIndex], aMemberKeyValues[tIndex + 1]);
		}
		return new Ini().new Section(aSectionName, tMembers);
	}

	private void checkIni(Ini aIni, Section... aExpectedSections) {
		Assert.assertEquals(aExpectedSections.length, aIni.getSectionCount());
		int tExpectedSectionIndex = 0;
		LinkedHashMap<String, Section> tSections = aIni.getSections();
		for (Entry<String, Section> tEntry : tSections.entrySet()) {
			Section tCurrentExpectedSection = aExpectedSections[tExpectedSectionIndex++];
			Assert.assertEquals(tCurrentExpectedSection.getSectionName(), tEntry.getKey());
			Assert.assertEquals(tCurrentExpectedSection.getSectionName(), tEntry.getValue().getSectionName());
			Assert.assertEquals(tCurrentExpectedSection.getParameterCount(), tEntry.getValue().getParameterCount());
			checkSectionMembers(tCurrentExpectedSection.getMembers(), tEntry.getValue().getMembers());
		}
	}

	private void checkSectionMembers(LinkedHashMap<String, String> aExpectedMembers, LinkedHashMap<String, String> aActualMembers) {
		Iterator<Entry<String, String>> tExpectedMemberIterator = aExpectedMembers.entrySet().iterator();
		Iterator<Entry<String, String>> tActualMemberIterator = aActualMembers.entrySet().iterator();
		while (tActualMemberIterator.hasNext()) {
			Entry<String, String> tExpectedEntry = tExpectedMemberIterator.next();
			Entry<String, String> tActualEntry = tActualMemberIterator.next();
			Assert.assertEquals(tExpectedEntry.getKey(), tActualEntry.getKey());
			Assert.assertEquals(tExpectedEntry.getValue(), tActualEntry.getValue());
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testIni() throws FileNotFoundException, IOException {
		Ini tIni = new Ini();
		tIni.load(this.getClass().getResourceAsStream("testdata.ini"));

		checkIni(tIni,
				createSection("jp",
						"dog", "1",
						"cat", "2",
						"caw", "5",
						"fish", "9",
						"bug", "9"),
				createSection("us",
						"dog", "10",
						"cat", "20"),
				createSection("cl",
						"caw", "15",
						"fish", "27",
						"bug", "27"));
	}

	@Test
	public void testIniDefaultSection() throws FileNotFoundException, IOException {
		Ini tIni = new Ini();
		tIni.load(this.getClass().getResourceAsStream("testdata-default.ini"));

		checkIni(tIni,
				createSection("default",
						"dog", "1",
						"cat", "2",
						"caw", "5",
						"fish", "9",
						"bug", "9"),
				createSection("us",
						"dog", "10",
						"cat", "20"),
				createSection("cl",
						"caw", "15",
						"fish", "27",
						"bug", "27"));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
