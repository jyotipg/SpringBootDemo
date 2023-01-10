package com.hsbc.gbm.surveillance.sdf.trade.processor.utils;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.annotation.PostConstruct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import com.hsbc.gbm.surveillance.sdf.trade.processor.config.SystemProperties;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.convertor.DataStandardColumnSetUniversalDescriptor;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.SchemaFields;
import com.hsbc.gbm.surveillance.sdf.trade.processor.rules.ColumnSetUniversalDescriptor;
import com.hsbc.gbm.surveillance.sdf.trade.processor.rules.DataWithRule;
import io.reactivex.Flowable;
import io.reactivex.subscribers.DisposableSubscriber;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
@AllArgsConstructor
@Data
public class UpdatedJSONDataConvertor {

	private SystemProperties systemProperties;
	private DataWithRule ruleInformation;

	private String mergeValues = "";

	@Autowired
	public UpdatedJSONDataConvertor(@Lazy SystemProperties systemProperties) {
		this.systemProperties = systemProperties;
	}

	@PostConstruct
	public void init() {
		System.out.println("systemProperties :--" + systemProperties);
		ruleInformation = JsonKeyConverter.convertJsonStringJavaObject(systemProperties.getJsonRuleFile(),
				DataWithRule.class);

	}

	public List<EcmDealogic> seemaDataEntityObject(String jsonString) {
		log.info("jsonString {}", jsonString);
		String businessDate = "20220507";
		List<SchemaFields> SchemaFieldsRows = jsonObjectProcessor(jsonString, businessDate);
		try {
			return EcmDealogicEntityConverter.convertToEcmDealogicEntity(SchemaFieldsRows);
		} catch (Exception e) {
			return null;
		}

	}

	@SuppressWarnings("unchecked")
	private List<SchemaFields> jsonObjectProcessor(String jsonString, String businessDate) {
		List<SchemaFields> dataRows = new ArrayList<SchemaFields>();
		JSONArray jsonArray = getJsonArray(jsonString, businessDate);
		log.info("Json Object {}", jsonArray);
		if (jsonArray != null) {

			Flowable.fromIterable(jsonArray).flatMap(jsonObj -> {

				return Flowable.just((JSONObject) jsonObj);
			}).subscribeWith(new DisposableSubscriber<JSONObject>() {

				@Override
				public void onNext(JSONObject jsonObject) {
					log.info("Json Object {}", jsonObject);
					ArrayList<DataStandardColumnSetUniversalDescriptor> dataColumnAndValues = new ArrayList<DataStandardColumnSetUniversalDescriptor>();
					Flowable.fromIterable(ruleInformation.getColumnSetUniversalDescriptor()).subscribe(schemaField -> {
						if (jsonObject.containsKey(schemaField.getCsvHeader())) {
							String validatedColumnValue = String.valueOf(jsonObject.get(schemaField.getCsvHeader()));

							validatedColumnValue = String
									.valueOf(renderingFormatedValue(validatedColumnValue, schemaField.getDataType()));
							dataColumnAndValues
									.add(getStandardizedColumnValues(schemaField, jsonObject, validatedColumnValue));

						}

					});

					dataRows.add(SchemaFields.builder().schemaFields(dataColumnAndValues).build());
				}

				@Override
				public void onError(Throwable t) {
					log.error("Error {}", t.getMessage());
					throw new RuntimeException(t.getMessage());
				}

				@Override
				public void onComplete() {
					log.info("Data Standarization is Completed");
				}

			});
		}
		return dataRows;
	}

	private JSONArray getJsonArray(String jsonString, String businessDate) {
		JSONParser parser = new JSONParser();
		JSONArray jsonArray = null;
		try {
			Object object = (Object) parser.parse(jsonString);
			jsonArray = (JSONArray) object;
		} catch (Exception e) {
			jsonArray = null;
		}
		return mergeColumns(setBusinessDate(jsonArray, businessDate),
				new ArrayList<String>(Arrays.asList("Date", "Time")), "Order_Date_Time");
	}

	@SuppressWarnings("unchecked")
	private JSONArray setBusinessDate(JSONArray jsonArray, String businessDate) {
		jsonArray.forEach(jsonObject -> {
			JSONObject jobj = (JSONObject) jsonObject;

			jobj.put("Business_Date", businessDate);
		});
		return jsonArray;

	}

	@SuppressWarnings("unchecked")
	private JSONArray mergeColumns(JSONArray jsonArray, ArrayList<String> columns, String columnHeaderName) {

		jsonArray.forEach(jsonObject -> {
			JSONObject jobj = (JSONObject) jsonObject;
			mergeValues = "";
			columns.forEach(column -> {

				mergeValues = mergeValues.concat(" ").concat(jobj.get(column).toString());
				jobj.remove(column);
			});
			jobj.put(columnHeaderName, mergeValues.trim());
		});
		return jsonArray;
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
				.colunmSampleData(schemaField.getColumSampleData())
				.columnBusinessType(schemaField.getColumnBusinessType()).dataType(schemaField.getDataType())
				.isMandatory(Boolean.valueOf(schemaField.getIsMandatory()))
				.isDesired(Boolean.valueOf(schemaField.getIsDesired()))
				.allowedNull(Boolean.valueOf(schemaField.getAllowedNull()))
				.allowedEmptyValue(Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				.applicableForMasking(Boolean.valueOf(schemaField.getApplicableForMasking()))
				.defaultValueRenderingFormat(schemaField.getDefaultValueRenderingFormat())
				.isColumnHidden(Boolean.valueOf(schemaField.getIsColumnHidden()))
				.isColumnPrimaryFilter(Boolean.valueOf(schemaField.getIsColumnPrimaryFilter()))
				.columnOriginalValue(String.valueOf(jsonObject.get(schemaField.getCsvHeader())))
				.formatedColumnValue(validatedColumnValue)
				.usedForCommonEnrichment(schemaField.getUsedForCommonEnrichment())
				.usedForMarketDataLinking(schemaField.getUsedForMarketDataLinking())
				.filteringRules(schemaField.getFilteringRules()).build();
	}

	private Object renderingFormatedValue(String value, String dataType) {
		if (dataType.trim().toLowerCase().equals("integer")) {
			return Integer.valueOf(value);
		}

		if (dataType.trim().toLowerCase().equals("date")) {

			return getDate(value);

		}

		if (dataType.trim().toLowerCase().equals("epochdatetime")) {

			return getEpochTime(value);

		}
		if (dataType.trim().toLowerCase().equals("double")) {

			return getDoubleValue(value);
		}
		if (dataType.trim().toLowerCase().equals("float")) {
			return getFloatValue(value);
		}
		return value;
	}

	private String getDoubleValue(String value) {
		String dataFormat = "#.00";
		try {
			double doubleValue = Double.valueOf(value);
			DecimalFormat df = new DecimalFormat(dataFormat);
			return df.format(doubleValue);
		} catch (Exception e3) {
			return "";

		}
	}

	private String getFloatValue(String value) {
		String dataFormat = "#.00";
		try {
			float floatValue = Float.valueOf(value);
			DecimalFormat df = new DecimalFormat(dataFormat);
			return df.format(floatValue);

		} catch (Exception e3) {
			return "";

		}
	}

	private String getDate(String value) {
		String dataFormat = "dd/MMM/yyyy";
		String dateValue = null;
		List<String> datepatern = Arrays.asList("MM-dd-yyyy", "yyyy-MM-dd", "yyyyMMdd");

		for (String pattern : datepatern) {
			try {
				dateValue = DateTimeFormatter.ofPattern(dataFormat)
						.format(LocalDate.parse(value, DateTimeFormatter.ofPattern(pattern)));
				break;
			} catch (Exception e) {
				dateValue = null;
			}
		}

		return dateValue;
	}

	private Long getEpochTime(String dateTime) {
		Long time = null;
		List<String> datepatern = Arrays.asList("d MMM yyyy HH:mm[:ss] [Z][z][ZZ][zz]",
				"d MM yyyy HH:mm[:ss] [Z][z][ZZ][zz]", "yyyy MMM d HH:mm[:ss] [Z][z][ZZ][zz]",
				"d/MMM/yyyy HH:mm[:ss] [Z][z][ZZ][zz]", "d/MM/yyyy HH:mm[:ss] [Z][z][ZZ][zz]",
				"yyyy/MM/d HH:mm[:ss] [Z][z][ZZ][zz]", "d-MMM-yyyy HH:mm[:ss] [Z][z][ZZ][zz]",
				"d-MM-yyyy HH:mm[:ss] [Z][z][ZZ][zz]", "yyyy-MM-d HH:mm[:ss] [Z][z][ZZ][zz]");

		for (String pattern : datepatern) {
			try {
				time = ZonedDateTime.parse(dateTime, new DateTimeFormatterBuilder().parseCaseInsensitive()
						.appendPattern(pattern).toFormatter(Locale.ROOT)).toInstant().toEpochMilli();
				break;
			} catch (Exception e) {
				time = null;
			}
		}

		return time;
	}
}