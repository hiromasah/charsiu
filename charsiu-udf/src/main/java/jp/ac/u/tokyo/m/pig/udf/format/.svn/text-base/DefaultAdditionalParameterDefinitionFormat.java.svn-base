package jp.ac.u.tokyo.m.pig.udf.format;

import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.data.type.AbstractPigTypeStringCaster;
import jp.ac.u.tokyo.m.pig.udf.FormatConstants;
import jp.ac.u.tokyo.m.string.StringUtil;

import org.apache.pig.data.DataType;

public class DefaultAdditionalParameterDefinitionFormat implements AdditionalParameterDefinitionFormat {

	// -----------------------------------------------------------------------------------------------------------------

	private final List<AdditionalParameterCaster> mAdditionalParameterCasters;
	private final List<String> mAliases;

	// -----------------------------------------------------------------------------------------------------------------

	public DefaultAdditionalParameterDefinitionFormat(
			String aAdditionalParameterDefinitionFormatString,
			String aWordSeparator,
			String aAdditionalParameterSeparator) {
		List<String> tAdditionalParameters = FormatUtil.splitGroup(aAdditionalParameterDefinitionFormatString,
				aWordSeparator,
				FormatConstants.SUB_GROUP_OPENER, FormatConstants.SUB_GROUP_CLOSER,
				FormatConstants.ADDITIONAL_PARAMETER_OPENER, FormatConstants.ADDITIONAL_PARAMETER_CLOSER);
		List<AdditionalParameterCaster> tAdditionalParameterCasters = new ArrayList<AdditionalParameterCaster>();
		List<String> tAliases = new ArrayList<String>();
		int tAdditionalParametersSize = tAdditionalParameters.size();
		for (int tIndex = 0; tIndex < tAdditionalParametersSize;) {
			String[] tTypeAlias = tAdditionalParameters.get(tIndex++).split(aAdditionalParameterSeparator);
			// 要素 1 なら alias のみ。型はデフォルトで chararray
			if (tTypeAlias.length == 1) {
				tAliases.add(tTypeAlias[0].trim());
				tAdditionalParameterCasters.add(StringAdditionalParameterCaster.INSTANCE);
			}
			// 要素 2 なら alias : type 。
			else {
				tAliases.add(tTypeAlias[0].trim());
				tAdditionalParameterCasters.add(getAdditionalParameterCaster(tTypeAlias[1]));
			}
		}
		mAdditionalParameterCasters = tAdditionalParameterCasters;
		mAliases = tAliases;
	}

	// -----------------------------------------------------------------------------------------------------------------

	private AdditionalParameterCaster getAdditionalParameterCaster(String aTypeString) {
		return TypeStringCasterPigToAdditionalParameterCaster.INSTANCE.castTypeString(aTypeString.trim());
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<AdditionalParameterCaster> getAdditionalParameterCasters() {
		return mAdditionalParameterCasters;
	}

	@Override
	public List<String> getAliases() {
		return mAliases;
	}

	@Override
	public AdditionalParameterCaster getDefaultAdditionalParameterCaster() {
		return StringAdditionalParameterCaster.INSTANCE;
	}

	// -----------------------------------------------------------------------------------------------------------------

	private static class TypeStringCasterPigToAdditionalParameterCaster extends AbstractPigTypeStringCaster<AdditionalParameterCaster> {
		static final TypeStringCasterPigToAdditionalParameterCaster INSTANCE = new TypeStringCasterPigToAdditionalParameterCaster();

		private TypeStringCasterPigToAdditionalParameterCaster() {}

		@Override
		public AdditionalParameterCaster caseDouble() {
			return DoubleAdditionalParameterCaster.INSTANCE;
		}

		@Override
		public AdditionalParameterCaster caseFloat() {
			return FloatAdditionalParameterCaster.INSTANCE;
		}

		@Override
		public AdditionalParameterCaster caseInt() {
			return IntegerAdditionalParameterCaster.INSTANCE;
		}

		@Override
		public AdditionalParameterCaster caseLong() {
			return LongAdditionalParameterCaster.INSTANCE;
		}

		@Override
		public AdditionalParameterCaster caseString() {
			return StringAdditionalParameterCaster.INSTANCE;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	public static class StringAdditionalParameterCaster implements AdditionalParameterCaster {
		private static final StringAdditionalParameterCaster INSTANCE = new StringAdditionalParameterCaster();

		private StringAdditionalParameterCaster() {}

		@Override
		public Object castParameter(String aParameterString) {
			return aParameterString;
		}

		@Override
		public byte getType() {
			return DataType.CHARARRAY;
		}
	}

	private static abstract class AbstractNumberAdditionalParameterCaster implements AdditionalParameterCaster {
		@Override
		public Object castParameter(String aParameterString) {
			if (aParameterString == null)
				return null;
			String tTrimedParameterString = StringUtil.trimMultiByteCharacter(aParameterString);
			try {
				return castNumber(tTrimedParameterString);
			} catch (NumberFormatException e) {
				if (tTrimedParameterString.length() == 0)
					return null;
				else
					throw e;
			}
		}

		abstract Object castNumber(String aParameterString);
	}

	public static class IntegerAdditionalParameterCaster extends AbstractNumberAdditionalParameterCaster {
		private static final IntegerAdditionalParameterCaster INSTANCE = new IntegerAdditionalParameterCaster();

		private IntegerAdditionalParameterCaster() {}

		@Override
		public Object castNumber(String aParameterString) {
			return new Integer(aParameterString);
		}

		@Override
		public byte getType() {
			return DataType.INTEGER;
		}
	}

	public static class LongAdditionalParameterCaster extends AbstractNumberAdditionalParameterCaster {
		private static final LongAdditionalParameterCaster INSTANCE = new LongAdditionalParameterCaster();

		private LongAdditionalParameterCaster() {}

		@Override
		public Object castNumber(String aParameterString) {
			return new Long(aParameterString);
		}

		@Override
		public byte getType() {
			return DataType.LONG;
		}
	}

	public static class FloatAdditionalParameterCaster extends AbstractNumberAdditionalParameterCaster {
		private static final FloatAdditionalParameterCaster INSTANCE = new FloatAdditionalParameterCaster();

		private FloatAdditionalParameterCaster() {}

		@Override
		public Object castNumber(String aParameterString) {
			return new Float(aParameterString);
		}

		@Override
		public byte getType() {
			return DataType.FLOAT;
		}
	}

	public static class DoubleAdditionalParameterCaster extends AbstractNumberAdditionalParameterCaster {
		private static final DoubleAdditionalParameterCaster INSTANCE = new DoubleAdditionalParameterCaster();

		private DoubleAdditionalParameterCaster() {}

		@Override
		public Object castNumber(String aParameterString) {
			return new Double(aParameterString);
		}

		@Override
		public byte getType() {
			return DataType.DOUBLE;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

}
