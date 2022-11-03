package com.hsbc.gbm.surveillance.sdf.trade.processor.utils;

import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.time.DateUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.DataEntity;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.DataStandardColumnSetUniversalDescriptor;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.JsonDataEntity;
import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.SchemaFields;
import com.hsbc.gbm.surveillance.sdf.trade.processor.rules.ColumnSetUniversalDescriptor;
import com.hsbc.gbm.surveillance.sdf.trade.processor.rules.DataWithRule;

import io.reactivex.Flowable;
import io.reactivex.subscribers.DisposableSubscriber;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@SuppressWarnings("all")
@RequiredArgsConstructor
public class JSONDataConvertor {

	@Value("${json.rule.file}")
	private String jsonRuleFile;
	
	private DataWithRule ruleInformation;
	private String mergeValues="";
	@PostConstruct
	public void init() {
		ruleInformation = convertJsonRuleToJavaObject(DataWithRule.class);
	}
	public List<SchemaFields> convertedJsonObject(String jsonString) {
		log.info("jsonString {}", jsonString);
		String businessDate="20220507";
		List<SchemaFields> dataRows = jsonObjectProcessor(jsonString,businessDate);
		List<JsonDataEntity> entityRows = generateEntity(dataRows);

		return dataRows;
	}

	public List<JsonDataEntity> convertedJsonDataEntityObject(String jsonString) {
		log.info("jsonString {}", jsonString);
		String businessDate="20220507";
		List<SchemaFields> dataRows = jsonObjectProcessor(jsonString,businessDate);
		List<JsonDataEntity> entityRows = generateEntity(dataRows);
		return entityRows;
	}
	
	public List<DataEntity> convertedDataEntityObject(String jsonString) {
		log.info("jsonString {}", jsonString);
		String businessDate="20220507";
		List<SchemaFields> dataRows = jsonObjectProcessor(jsonString,businessDate);
		List<JsonDataEntity> entityRows = generateEntity(dataRows);
		/**
		 * This Enitity will store in database, so We need to convert this DataEntity to JPA Entity (which is created by Seema)
		 * **/
		return getDataEntity(entityRows,businessDate);
	}
	
	public List<EcmDealogic>seemaDataEntityObject(String jsonString) {
		log.info("jsonString {}", jsonString);
		String businessDate="20220507";
		List<SchemaFields> SchemaFieldsRows = jsonObjectProcessor(jsonString,businessDate);
		try {
			return EcmDealogicEntityConverter.convertToEcmDealogicEntity(SchemaFieldsRows);
		} catch (Exception e) {
		return null;
		}
		
		
	}
	private List<SchemaFields> jsonObjectProcessorOld(String jsonString, String businessDate) {
		List<SchemaFields> dataRows = new ArrayList<SchemaFields>();
		JSONArray jsonArray = getJsonArray(jsonString,businessDate);
		List<String> errorList = new ArrayList<String>();
		if (jsonArray != null) {

			Flowable.fromIterable(jsonArray).flatMap(jsonObj -> {

				return Flowable.just((JSONObject) jsonObj);
			}).subscribeWith(new DisposableSubscriber<JSONObject>() {

				@Override
				public void onNext(JSONObject jsonObject) {
				 	log.info("Json Object {}",jsonObject);
					ArrayList<DataStandardColumnSetUniversalDescriptor> dataColumnAndValues = new ArrayList<DataStandardColumnSetUniversalDescriptor>();
					Flowable.fromIterable(ruleInformation.getSchemaFields()).subscribe(schemaField -> {
						 if (jsonObject.containsKey(schemaField.getColumnOriginalName())) {
							String validatedColumnValue = removeSpecificChars(
									String.valueOf(jsonObject.get(schemaField.getColumnOriginalName())),
									schemaField.getDataType().trim().toLowerCase());
							validateRules(validatedColumnValue, schemaField, errorList);
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
					if (!errorList.isEmpty()) {
						// throw new RuntimeException(errorList.toString());
					}
				}

			});
		}
		return dataRows;
	}

	private List<SchemaFields> jsonObjectProcessor(String jsonString, String businessDate) {
		List<SchemaFields> dataRows = new ArrayList<SchemaFields>();
		JSONArray jsonArray = getJsonArray(jsonString,businessDate);
		List<String> errorList = new ArrayList<String>();
		if (jsonArray != null) {

			Flowable.fromIterable(jsonArray).flatMap(jsonObj -> {

				return Flowable.just((JSONObject) jsonObj);
			}).subscribeWith(new DisposableSubscriber<JSONObject>() {

				@Override
				public void onNext(JSONObject jsonObject) {
				 	log.info("Json Object {}",jsonObject);
					ArrayList<DataStandardColumnSetUniversalDescriptor> dataColumnAndValues = new ArrayList<DataStandardColumnSetUniversalDescriptor>();
					Flowable.fromIterable(ruleInformation.getSchemaFields()).subscribe(schemaField -> {
						if (jsonObject.containsKey(schemaField.getCsvHeader())) {
							String validatedColumnValue = removeSpecificChars(
									String.valueOf(jsonObject.get(schemaField.getCsvHeader())),
									schemaField.getDataType().trim().toLowerCase());
							validateRules(validatedColumnValue, schemaField, errorList);
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
					if (!errorList.isEmpty()) {
						// throw new RuntimeException(errorList.toString());
					}
				}

			});
		}
		return dataRows;
	}
	private JSONArray getJsonArray(String jsonString,String businessDate) {
		JSONParser parser = new JSONParser();
		JSONArray jsonArray = null;
		try {
			Object object = (Object) parser.parse(jsonString);
			jsonArray = (JSONArray) object;
		} catch (Exception e) {
			jsonArray = null;
		}
		return mergeColumns(setBusinessDate(jsonArray,businessDate), new ArrayList<String>(Arrays.asList("Date","Time")),"Order_Date_Time");
	}
 
	private JSONArray setBusinessDate(JSONArray jsonArray,String businessDate)
	{
			jsonArray.forEach(jsonObject->{
				JSONObject jobj = (JSONObject)jsonObject;
						
				jobj.put("Business_Date", businessDate);	
				});			  
			return jsonArray;
	
	}
	private JSONArray mergeColumns(JSONArray jsonArray, ArrayList<String> columns , String columnHeaderName)
	{
		
		jsonArray.forEach(jsonObject->{
			JSONObject jobj = (JSONObject)jsonObject;
			mergeValues="";
			columns.forEach(column->{
				
				mergeValues=mergeValues.concat(" ").concat(jobj.get(column).toString());
				jobj.remove(column);
			});
			jobj.put(columnHeaderName, mergeValues.trim());
		});
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
	 
	private List<String> validateRules(String validatedColumnValue, ColumnSetUniversalDescriptor schemaField,
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
				.columnOriginalValue(String.valueOf(jsonObject.get(schemaField.getCsvHeader())))
				.formatedColumnValue(validatedColumnValue)
				.usedForCommonEnrichment(schemaField.getUsedForCommonEnrichment())
				.usedForMarketDataLinking(schemaField.getUsedForMarketDataLinking())
				.filteringRules(schemaField.getFilteringRules()).build();
	}
	 
	private List<DataEntity> getDataEntity(List<JsonDataEntity> entities, String businessDate) {
		Field[] jsonDataEntityFields = JsonDataEntity.class.getDeclaredFields();
		List<DataEntity> dataEntityList = new ArrayList<DataEntity>();
		entities.stream().forEach(entity -> {

			try {
				DataEntity dataEntity = new DataEntity();				
				
				for (Field f : jsonDataEntityFields) {

					Field field = entity.getClass().getDeclaredField(f.getName());
					field.setAccessible(true);
					Object value = field.get(entity);
					try {
						Field en = DataEntity.class.getDeclaredField(f.getName());
						en.setAccessible(true);
						if (value==null)
						{
							en.set(dataEntity, null);
						}
						else
						{
							if (en.getType().toString().toLowerCase().endsWith("string")) {
								en.set(dataEntity, String.valueOf(value));
							}
							if (en.getType().toString().toLowerCase().endsWith("integer")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity,  Integer.valueOf(decimalValue));
							}	
							if (en.getType().toString().toLowerCase().endsWith("double")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity,  Double.valueOf(decimalValue));
								
							}	
							if (en.getType().toString().toLowerCase().endsWith("float")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity, Float.valueOf(decimalValue));
							}						
							if (en.getType().toString().toLowerCase().endsWith("bigdecimal")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity, new BigDecimal(Float.valueOf(decimalValue)));
							}
							
							if (en.getType().toString().toLowerCase().endsWith("date")) {

								Optional<ColumnSetUniversalDescriptor> role = ruleInformation.getSchemaFields().stream()
										.filter(d -> d.getColumnStandardizedName().toLowerCase()
												.equals(f.getName().toLowerCase()))
										.findFirst();
								
								String dateFormat="dd/MMM/yyyy";
								if (role.isPresent()) {
									dateFormat=role.get().getDefaultValueRenderingFormat();
								}  
								try {
									en.set(dataEntity,  DateUtils.addDays(new SimpleDateFormat(dateFormat).parse(String.valueOf(value)), 1));
								} catch (Exception e) {
									e.printStackTrace();
									en.set(dataEntity, null);
								}

							}
							
							if (en.getType().toString().toLowerCase().endsWith("timestamp")) {

								Optional<ColumnSetUniversalDescriptor> role = ruleInformation.getSchemaFields().stream()
										.filter(d -> d.getColumnStandardizedName().toLowerCase()
												.equals(f.getName().toLowerCase()))
										.findFirst();
								
								String dateFormat="dd/MMM/yyyy hh:mm:ss.SSS";
								if (role.isPresent()) {
									dateFormat=role.get().getDefaultValueRenderingFormat();
								}  
								try {
									en.set(dataEntity,  new Timestamp(DateUtils.addDays(new SimpleDateFormat(dateFormat).parse(String.valueOf(value)), 1).getTime()));
								} catch (Exception e) {
									e.printStackTrace();
									en.set(dataEntity, null);
								}

							}							
						}
						
					} catch (Exception e) {
						e.printStackTrace();
					}


				}
				/**
				 * Temporary added business date
				 * */
				//dataEntity.setBusinessDate(new Date());
				dataEntityList.add(dataEntity);
			} catch (Exception e) {
				e.printStackTrace();
			}

		});
		return dataEntityList;
	}
	 
	private List<DataEntity> getGenericDataEntity(List<JsonDataEntity> entities, String businessDate) {
		Field[] jsonDataEntityFields = JsonDataEntity.class.getDeclaredFields();
		List<DataEntity> dataEntityList = new ArrayList<DataEntity>();
		entities.stream().forEach(entity -> {

			try {
				DataEntity dataEntity = new DataEntity();
				for (Field f : jsonDataEntityFields) {

					Field field = entity.getClass().getDeclaredField(f.getName());
					field.setAccessible(true);
					Object value = field.get(entity);
					try {
						Field en = DataEntity.class.getDeclaredField(f.getName());
						en.setAccessible(true);
						if (value==null)
						{
							en.set(dataEntity, null);
						}
						else
						{
							if (en.getType().toString().toLowerCase().endsWith("string")) {
								en.set(dataEntity, String.valueOf(value));
							}
							if (en.getType().toString().toLowerCase().endsWith("integer")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity,  Integer.valueOf(decimalValue));
							}	
							if (en.getType().toString().toLowerCase().endsWith("double")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity,  Double.valueOf(decimalValue));
								
							}	
							if (en.getType().toString().toLowerCase().endsWith("float")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity, Float.valueOf(decimalValue));
							}						
							if (en.getType().toString().toLowerCase().endsWith("bigdecimal")) {
								String decimalValue = String.valueOf(value).replaceAll(",", "");
								en.set(dataEntity, new BigDecimal(Float.valueOf(decimalValue)));
							}
							
							if (en.getType().toString().toLowerCase().endsWith("date")) {

								Optional<ColumnSetUniversalDescriptor> role = ruleInformation.getSchemaFields().stream()
										.filter(d -> d.getColumnStandardizedName().toLowerCase()
												.equals(f.getName().toLowerCase()))
										.findFirst();
								
								String dateFormat="dd/MMM/yyyy";
								if (role.isPresent()) {
									dateFormat=role.get().getDefaultValueRenderingFormat();
								}  
								try {
									en.set(dataEntity,  DateUtils.addDays(new SimpleDateFormat(dateFormat).parse(String.valueOf(value)), 1));
								} catch (Exception e) {
									e.printStackTrace();
									en.set(dataEntity, null);
								}

							}
							
							if (en.getType().toString().toLowerCase().endsWith("timestamp")) {

								Optional<ColumnSetUniversalDescriptor> role = ruleInformation.getSchemaFields().stream()
										.filter(d -> d.getColumnStandardizedName().toLowerCase()
												.equals(f.getName().toLowerCase()))
										.findFirst();
								
								String dateFormat="dd/MMM/yyyy hh:mm:ss.SSS";
								if (role.isPresent()) {
									dateFormat=role.get().getDefaultValueRenderingFormat();
								}  
								try {
									en.set(dataEntity,  new Timestamp(DateUtils.addDays(new SimpleDateFormat(dateFormat).parse(String.valueOf(value)), 1).getTime()));
								} catch (Exception e) {
									e.printStackTrace();
									en.set(dataEntity, null);
								}

							}							
						}
						
					} catch (Exception e) {
						e.printStackTrace();
					}


				}
				/**
				 * Temporary added business date
				 * */
				//dataEntity.setBusinessDate(new Date());
				dataEntityList.add(dataEntity);
			} catch (Exception e) {
				e.printStackTrace();
			}

		});
		return dataEntityList;
	}

	private List<JsonDataEntity> generateEntity(List<SchemaFields> schemaField) {
		List<JsonDataEntity> jsonDataEntityList = new ArrayList<JsonDataEntity>();
		schemaField.stream().forEach(fields -> {
			JsonDataEntity entity = new JsonDataEntity();
			fields.getSchemaFields().stream().forEach(datastandardRow -> {
				getJsonDataEntity(datastandardRow, entity);
			});

			jsonDataEntityList.add(entity);
		});
		return jsonDataEntityList;
	}

	private void getJsonDataEntity(DataStandardColumnSetUniversalDescriptor datastandardRow, JsonDataEntity entity) {
		try {
			Field[] declaredFields = JsonDataEntity.class.getDeclaredFields();
			for (Field f : declaredFields) {

				if (datastandardRow.getColumnStandardizedName().trim().toLowerCase()
						.equals(f.getName().trim().toLowerCase())) {
					f.setAccessible(true);
					f.set(entity, String.valueOf(datastandardRow.getFormatedColumnValue()));
					break;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

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
					 
					SimpleDateFormat inSDF = new SimpleDateFormat("MM-dd-yyyy");
					Date date = inSDF.parse(value);
					SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
					String outDate = dateFormat.format(date);
					return dateFormat.format(dateFormat.parse(outDate));
				} catch (Exception e2) {
					try {
						SimpleDateFormat inSDF = new SimpleDateFormat("yyyy-MM-dd");
						Date date = inSDF.parse(value);
						SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
						String outDate = dateFormat.format(date);
						return dateFormat.format(dateFormat.parse(outDate));
					} catch (Exception e3) {
					
						try {
							
							String dFormat="yyyyMMdd";
							Date date=new SimpleDateFormat(dFormat).parse(value);	
							
							SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
							String outDate = dateFormat.format(date);
							return dateFormat.format(dateFormat.parse(outDate));
						} catch (Exception e4) {
							return "";
						}
					}

				}

			}
		}
		if (dataType.trim().toLowerCase().equals("datetime")) {

			try {
				
				SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
				Date parsedDate = inputFormat.parse(value);
				SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
				return dateFormat.format(parsedDate); 
			} catch (Exception e) {
				
				try {
					SimpleDateFormat inputFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
					Date parsedDate = inputFormat.parse(value);
					SimpleDateFormat dateFormat = new SimpleDateFormat(dataFormat);
					return dateFormat.format(parsedDate); 
				} catch (Exception e2) {
					try {
						Instant instant = Instant.parse(value);
						LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of(ZoneOffset.UTC.getId()));
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
}