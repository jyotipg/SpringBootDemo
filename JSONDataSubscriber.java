package com.hsbc.gbm.surveillance.sdf.trade.processor.utils;

import java.io.File;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.DisposableSubscriber;

import javax.annotation.PostConstruct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.DataStandardColumnSetUniversalDescriptor;
import com.hsbc.gbm.surveillance.sdf.trade.processor.rules.ColumnSetUniversalDescriptor;
import com.hsbc.gbm.surveillance.sdf.trade.processor.rules.DataWithRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@SuppressWarnings("all")
@RequiredArgsConstructor
public class JSONDataSubscriber {

	@Value("${json.rule.file}")
	private String jsonRuleFile;

	private DataWithRule ruleInformation;
	private Map<Integer, List<DataStandardColumnSetUniversalDescriptor>> dataRows = new HashMap<Integer, List<DataStandardColumnSetUniversalDescriptor>>();
	private Map<Integer, List<DataWithRule>> csvdataWithForSqlQuery = new HashMap<Integer, List<DataWithRule>>();

	@PostConstruct
	public void init() {
		ruleInformation = convertJsonRuleToJavaObject(DataWithRule.class);
	}
	public Map<Integer, List<DataStandardColumnSetUniversalDescriptor>> convertedJsonObject(String jsonString) {
		log.info("jsonString {}", jsonString);
		jsonObjectProcessorSubscribe(jsonString);
		return dataRows;
	}
	
	 
	private void jsonObjectProcessorSubscribe(String jsonString) {
 
		List<String> errorList = new ArrayList<String>();
		if (StringUtils.hasLength(jsonString)) {
			JSONArray jsonArray = getJsonArray(jsonString);
			if (jsonArray != null) {
				 
				Flowable.fromIterable(jsonArray).subscribe(jsonObj -> {
					ArrayList<DataStandardColumnSetUniversalDescriptor> dataColumnAndValues = new ArrayList<DataStandardColumnSetUniversalDescriptor>();
					JSONObject jsonObject = (JSONObject) jsonObj;
					Flowable.fromIterable(ruleInformation.getSchemaFields()).subscribe(schemaField -> {
						if (jsonObject.containsKey(schemaField.getColumnOriginalName())) {
							String validatedColumnValue = removeSpecificChars(
									String.valueOf(jsonObject.get(schemaField.getColumnOriginalName())),
									schemaField.getDataType().trim().toLowerCase());
							 validateRules(validatedColumnValue, schemaField, errorList);
							if (!errorList.isEmpty()) {
								// throw new RuntimeException(errorList.toString());
							}
							validatedColumnValue = String.valueOf(renderingFormatedValue(validatedColumnValue,
									schemaField.getDataType(), schemaField.getDefaultValueRenderingFormat()));
							dataColumnAndValues
									.add(getStandardizedColumnValues(schemaField, jsonObject, validatedColumnValue));
						}

					});

					dataRows.put((dataRows.isEmpty()) ? 0 : dataRows.size(), dataColumnAndValues);
				});
			}

		}
	}

	private JSONArray getJsonArray(String jsonString) {
		JSONParser parser = new JSONParser();
		JSONArray jsonArray = null;
		try {
			Object object = (Object) parser.parse(jsonString);
			jsonArray = (JSONArray) object;
		} catch (Exception e) {
			jsonArray = null;
		}
		return jsonArray;

	}

	private <T> T convertJsonRuleToJavaObject(Class<T> classType) {

		T t = null;

		try {
			File file = new ClassPathResource(jsonRuleFile).getFile();
			ObjectMapper mapper = new ObjectMapper();
			mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			t = mapper.readValue(file, classType);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(jsonRuleFile.concat(" file not found or invalid."));
		}

		return t;
	}
	private List<String> validateRules(String validatedColumnValue,ColumnSetUniversalDescriptor schemaField,List<String> errorList)
	{
		if (Boolean.valueOf(schemaField.getIsMandatory())) {
			if (validatedColumnValue == null && !Boolean.valueOf(schemaField.getAllowedNull()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName())
						.concat(" is null"));
			else if (validatedColumnValue.isEmpty()
					&& !Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName())
						.concat(" is empty"));
		} else {
			if (validatedColumnValue == null && !Boolean.valueOf(schemaField.getAllowedNull()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName())
						.concat(" is null"));
			else if (validatedColumnValue.isEmpty()
					&& !Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName())
						.concat(" is empty"));
		}
		return errorList;
	}
	private DataStandardColumnSetUniversalDescriptor getStandardizedColumnValues(
			ColumnSetUniversalDescriptor schemaField, JSONObject jsonObject, String validatedColumnValue) {
		return DataStandardColumnSetUniversalDescriptor.builder().columnId(schemaField.getColumnId())
				.allowedEmptyValue(Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				.allowedNull(Boolean.valueOf(schemaField.getAllowedNull()))
				.columnOriginalName(schemaField.getColumnOriginalName()).columnOrigin(schemaField.getColumnOrigin())
				.columnStandardizedName(schemaField.getColumnStandardizedName())
				.columnOrder(schemaField.getColumnOrder())
				.colunmNameAlternativeLabels(schemaField.getColunmNameAlternativeLabels())
				.columnNameVerboseLabel(schemaField.getColumnNameVerboseLabel())
				.colunmDescription(schemaField.getColunmDescription())
				.colunmSampleData(schemaField.getColunmSampleData())
				.columnBusinessType(schemaField.getColumnBusinessType()).dataType(schemaField.getDataType())
				.isMandatory(Boolean.valueOf(schemaField.getIsMandatory()))
				.isDesired(Boolean.valueOf(schemaField.getIsDesired()))
				.allowedNull(Boolean.valueOf(schemaField.getAllowedNull()))
				.allowedEmptyValue(Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				.applicableForMasking(Boolean.valueOf(schemaField.getApplicableForMasking()))
				.defaultValueRenderingFormat(schemaField.getDefaultValueRenderingFormat())
				.isColumnHidden(Boolean.valueOf(schemaField.getIsColumnHidden()))
				.isColumnPrimaryFilter(Boolean.valueOf(schemaField.getIsColumnPrimaryFilter()))
				.columnOriginalValue(String.valueOf(jsonObject.get(schemaField.getColumnOriginalName())))
				.formatedColumnValue(validatedColumnValue)
				.usedForCommonEnrichment(schemaField.getUsedForCommonEnrichment())
				.usedForMarketDataLinking(schemaField.getUsedForMarketDataLinking())
				.filteringRules(schemaField.getFilteringRules()).build();
	}

	private String removeSpecificChars(String originalstring, String datType) {
		String removecharacterstring = "";
		if (datType.toLowerCase().trim().equals("keyword")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><.,;:''{}\\\"";
		}
		if (datType.toLowerCase().trim().equals("string") || datType.toLowerCase().trim().equals("integer")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><.,;:''{}\\\\\\\"";
		}
		if (datType.toLowerCase().trim().equals("float") || datType.toLowerCase().trim().equals("double")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><,;:''{}\\\\\\\"";
		}
		if (datType.toLowerCase().trim().equals("date")) {
			String text = originalstring;

			text = text.replaceAll("/", "-");
			text = text.replaceAll("[^a-zA-Z0-9\\\\s+^-]", "");
			text = text.replaceAll("\\\\", "");
			text = text.replaceAll("/", "-");
			return text.replaceAll("--", "-").trim();
		}
		if (datType.toLowerCase().trim().equals("time")) {
			removecharacterstring = "~!@#$%^&*()-_+=|\\?//><.,;''{}\\\\\\\\\\\\\\\"";
		}
		char[] orgchararray = originalstring.toCharArray();
		char[] removechararray = removecharacterstring.toCharArray();
		int start, end = 0;
		boolean[] tempBoolean = new boolean[128];
		for (start = 0; start < removechararray.length; ++start) {
			tempBoolean[removechararray[start]] = true;
		}
		for (start = 0; start < orgchararray.length; ++start) {
			if (!tempBoolean[orgchararray[start]]) {
				orgchararray[end++] = orgchararray[start];
			}
		}

		return new String(orgchararray, 0, end).trim();
	}

	private Object renderingFormatedValue(String value, String dataType, String dataFormat) {

		if (dataType.trim().toLowerCase().equals("integer")) {
			return Integer.valueOf(value);
		}
		if (dataType.trim().toLowerCase().equals("date")) {
			try {
				if (!dataFormat.isEmpty()) {
					SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
					return dateFormat.format(dateFormat.parse(value));
				}

				return value;
			} catch (Exception e) {

				try {

					SimpleDateFormat inSDF = new SimpleDateFormat("mm-dd-yyyy");
					Date date = inSDF.parse(value);
					SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
					String outDate = dateFormat.format(date);
					return dateFormat.format(dateFormat.parse(outDate));
				} catch (Exception e2) {
					try {
						SimpleDateFormat inSDF = new SimpleDateFormat("yyyy-mm-dd");
						Date date = inSDF.parse(value);
						SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
						String outDate = dateFormat.format(date);
						return dateFormat.format(dateFormat.parse(outDate));
					} catch (Exception e3) {
						return null;
					}

				}

			}
		}
		if (dataType.trim().toLowerCase().equals("datetime")) {

			try {
				Instant instant = Instant.parse(value);

				Date date = Date.from(instant);
				SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
				return dateFormat.format(date);

			} catch (Exception e3) {
				return null;

			}
		}
		if (dataType.trim().toLowerCase().equals("epochdatetime")) {

			try {
				Instant instant = Instant.parse(value);

				Date date = Date.from(instant);
				return date.getTime();

			} catch (Exception e3) {
				return null;

			}
		}
		if (dataType.trim().toLowerCase().equals("double")) {

			try {
				double doubleValue = Double.valueOf(value);
				DecimalFormat df = new DecimalFormat(dataFormat);
				return df.format(doubleValue);
			} catch (Exception e3) {
				return null;

			}
		}
		if (dataType.trim().toLowerCase().equals("float")) {

			try {
				float floatValue = Float.valueOf(value);
				DecimalFormat df = new DecimalFormat(dataFormat);
				return df.format(floatValue);

			} catch (Exception e3) {
				return null;

			}
		}
		return value;
	}
}