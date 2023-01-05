package com.hsbc.gbm.surveillance.sdf.trade.processor.utils;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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
	public UpdatedJSONDataConvertor( @Lazy SystemProperties systemProperties) {
		this.systemProperties=systemProperties;
	}
	@PostConstruct
	public void init() {
		System.out.println("systemProperties :--"+systemProperties);
	 	ruleInformation = JsonKeyConverter.convertJsonStringJavaObject(systemProperties.getJsonRuleFile(), DataWithRule.class);
	 
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
									 
									
							
							validatedColumnValue = String.valueOf(renderingFormatedValue(validatedColumnValue,
									schemaField.getDataType(), schemaField.getDefaultValueRenderingFormat()));
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
			System.out.println("MERGE-VALUES :----------------------------------------------------------------------------------------- "+mergeValues.trim());
			jobj.put(columnHeaderName, mergeValues.trim());
		});
		return jsonArray;
	}

	/*private List<String> validateRules(String validatedColumnValue, ColumnSetUniversalDescriptor schemaField,
			List<String> errorList) {
		if (Boolean.valueOf(schemaField.getIsMandatory())) {
			if (validatedColumnValue == null && !Boolean.valueOf(schemaField.getAllowedNull()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName()).concat(" is null"));
			else if (validatedColumnValue.isEmpty() && !Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName()).concat(" is empty"));
		} else {
			if (validatedColumnValue == null && !Boolean.valueOf(schemaField.getAllowedNull()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName()).concat(" is null"));
			else if (validatedColumnValue.isEmpty() && !Boolean.valueOf(schemaField.getAllowedEmptyValue()))
				errorList.add("column value of ".concat(schemaField.getColumnOriginalName()).concat(" is empty"));
		}
		return errorList;
	}*/

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
	 

	private Object renderingFormatedValue(String value, String dataType, String dataFormat) {
		if (dataType.trim().toLowerCase().equals("integer")) {
			return Integer.valueOf(value);
		}
		if (dataType.trim().toLowerCase().equals("zonedatetimestring")) {
			return zoneDateTimeString(value, dataFormat);
		}
		
		if (dataType.trim().toLowerCase().equals("date")) {

			try {
				if (!dataFormat.isEmpty()) {
				 
					return DateTimeFormatter.ofPattern(dataFormat).format(LocalDate.parse(value, DateTimeFormatter.ofPattern(dataFormat)));
				}

				return value;
			} catch (Exception e) {

				try {
					 
					return DateTimeFormatter.ofPattern(dataFormat).format(LocalDate.parse(value, DateTimeFormatter.ofPattern("MM-dd-yyyy")));
				} catch (Exception e2) {
					try {
						
						return DateTimeFormatter.ofPattern(dataFormat).format(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd")));
					} catch (Exception e3) {

						try {
							 
							return DateTimeFormatter.ofPattern(dataFormat).format(LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyyMMdd")));
						} catch (Exception e4) {
							return "";
						}
					}

				}

			}
		}
		if (dataType.trim().toLowerCase().equals("datetime")) {

			try {
				
				return DateTimeFormatter.ofPattern(dataFormat).format(LocalDate.parse(value, DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss")));
			} catch (Exception e) {

				try {
					
					return DateTimeFormatter.ofPattern(dataFormat).format(LocalDate.parse(value, DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss")));
				} catch (Exception e2) {
					try {
						Instant instant = Instant.parse(value);
						LocalDateTime localDateTime = LocalDateTime.ofInstant(instant,
								ZoneId.of(ZoneOffset.UTC.getId()));
						System.out.println(localDateTime.toString());
						Date parsedDate = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
						SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
						return dateFormat.format(parsedDate);
					} catch (Exception e3) {
						return "";
					}
				}

			}

		}
		if (dataType.trim().toLowerCase().equals("epochdatetime")) {

			try {
				Instant instant = Instant.parse(value);

				Date date = Date.from(instant);
				return date.getTime();

			} catch (Exception e3) {
				return "";

			}
		}
		if (dataType.trim().toLowerCase().equals("double")) {

			try {
				double doubleValue = Double.valueOf(value);
				DecimalFormat df = new DecimalFormat(dataFormat);
				return df.format(doubleValue);
			} catch (Exception e3) {
				return "";

			}
		}
		if (dataType.trim().toLowerCase().equals("float")) {

			try {
				float floatValue = Float.valueOf(value);
				DecimalFormat df = new DecimalFormat(dataFormat);
				return df.format(floatValue);

			} catch (Exception e3) {
				return "";

			}
		}
		return value;
	}
	private String zoneDateTimeString(String dateTimeValue, String format) {
		if (dateTimeValue.isEmpty())
			return null;
		 List<String> zoneWiseDate=List.of(dateTimeValue.split(" "));
	     String dateValue=zoneWiseDate.get(0).concat(" ").concat(zoneWiseDate.get(1));
	     String timeZone=zoneWiseDate.get(2);
	     
		SimpleDateFormat dateFormat = new SimpleDateFormat(format.concat(" a z"));
		Date date=convertDate(dateValue, format);
		try {
			System.out.println(date);
			if (date == null) return null;
			if (timeZone == null || "".equalsIgnoreCase(timeZone.trim())) {
				timeZone = Calendar.getInstance().getTimeZone().getID();
			}
			dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
			Timestamp ts = new Timestamp(((java.util.Date)dateFormat.parse(dateFormat.format(date))).getTime());
			System.out.println(ts);
		} catch (Exception e) {
			return null;
		}
	
		return dateFormat.format(date);
	}
	private Date convertDate(String dateValue, String format)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);
		try {			
			return dateFormat.parse(dateValue);
		 
		} catch (Exception e) {
		return null;
		}
	}
	private String trim(String value)
	{
		String  specialCharacter="`~!@#$%^&*()_+-=|\\][{}:;<>,./?'\"";
		String result=value;
		for (char c:specialCharacter.toCharArray())
		{  			
			result=trim(result,c);
		}		
		return result;
		
	}

	private String trim(String value, char c) {

    if (c <= 32) return value.trim();
    int len = value.length();
    int st = 0;
    char[] val = value.toCharArray();     

    while ((st < len) && (val[st] == c)) {
        st++;
    }
    while ((st < len) && (val[len - 1] == c)) {
        len--;
    }
    return ((st > 0) || (len < value.length())) ? value.substring(st, len) : value;
   } 

}