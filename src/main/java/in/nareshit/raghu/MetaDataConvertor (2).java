package com.hsbc.gbm.surveillance.sdf.trade.processor.convertor;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hsbc.gbm.surveillance.sdf.trade.processor.dto.MetaData;
import com.hsbc.gbm.surveillance.sdf.trade.processor.entity.MetaDataEntity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class MetaDataConvertor {  // @Mock

	@Value("${meta.data.file.info}")
	private String metadataFileInfo;

	@Value("$meta.data.yaml.file.info}")
	private String metadataYamlFileInfo;

	
	
	private final String META_DATA_FILE_NAME="fileNames";
	private ServiceFactory serviceFactory;
	
	
	@Autowired
	public MetaDataConvertor( @Lazy ServiceFactory serviceFactory) {
		this.serviceFactory=serviceFactory;
		
	}

	public void startProcess() {

		Arrays.asList(String.valueOf(convertYamlToJavaObject(metadataFileInfo, Map.class).get(META_DATA_FILE_NAME)).split(","))
				.stream().forEach(metaDataFile -> {

					MetaData metaData =convertFileToJavaObject(String.valueOf(metaDataFile), MetaData.class);
					if (metaData != null) {
						
						serviceFactory.getEcmMetaDataService().findProductIdAndSave(getMetaDataEntiry(metaData));
						
					}

				});

	}
	public void startYmlProcess() {

		Arrays.asList(String.valueOf(convertYamlToJavaObject(metadataYamlFileInfo, Map.class).get(META_DATA_FILE_NAME)).split(","))
				.stream().forEach(metaDataFile -> {

					MetaData metaData = convertYamlToJavaObject(String.valueOf(metaDataFile), MetaData.class);
					log.info("meta data {}",metaData.getColumnSchema());
					if (metaData != null) {
						serviceFactory.getEcmMetaDataService().findProductIdAndSave(getMetaDataEntiry(metaData));
						
					}

				});

	}
	private <T> T convertFileToJavaObject(String refereceFile, Class<T> classType) {

		T t = null;

		try {
			File file = new ClassPathResource(refereceFile).getFile();
			ObjectMapper mapper = new ObjectMapper();
				mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			t = mapper.readValue(file, classType);
		} catch (Exception e) {
			log.error("Exception {}",e.getMessage());
			t = null;
		}

		return t;
	}
	private <T> T convertYamlToJavaObject(String refereceFile, Class<T> classType) {

		T t = null;

		try {
			File file = new ClassPathResource(refereceFile).getFile();
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			t = mapper.readValue(file, classType);
			log.info("meta data added in metaData Dto");
		} catch (Exception e) {
			log.error("Exception {}",e.getMessage());
			t = null;
		}
		
		return t;
	}
	
	private  MetaDataEntity getMetaDataEntiry(MetaData metaData)
	{
		return MetaDataEntity.builder()
				.dataProductId(metaData.getDataProductId())
				.dataProductName(metaData.getDataProductName())
				.alternativeDataProductName(metaData.getAlternativeDataProductName())
				.dataProductCategory(metaData.getDataProductCategory())
				.typeOfData(metaData.getTypeOfData())
				.assetClass(metaData.getAssetClass())
				.productType(metaData.getProductType())
				.region(metaData.getRegion())
				.businessComment(metaData.getBusinessComment())
				.associatedRuleset(metaData.getAssociatedRuleset())
				.associatedManualControl(metaData.getAssociatedManualControl())
				.associatedDataProduct(metaData.getAssociatedDataProduct())
				.dataConsumer(metaData.getDataConsumer())
				.sdfFeddName(metaData.getSdfFeddName())
				.originalSourceSystem(metaData.getOriginalSourceSystem())
				.eim(metaData.getEim())
				.refreshFrequency(metaData.getRefreshFrequency())
				.lookbackPeriod(metaData.getLookbackPeriod())
				.retentionPolicyPeriod(metaData.getRetentionPolicyPeriod())
				.sourcingFlow(metaData.getSourcingFlow())
				.deliverySchedule(metaData.getDeliverySchedule())
				.sdfDocumentation(metaData.getSdfDocumentation())
				.storageLocationPath(metaData.getStorageLocationPath())
				.technicalComment(metaData.getTechnicalComment())
				.feedContact(metaData.getFeedContact())
				.sdfContact(metaData.getSdfContact())
				.supersededBy(metaData.getSupersededBy())
				.defaultDisplayDateRange(metaData.getDefaultDisplayDateRange())
				.defaultDisplayHiddenColumns(metaData.getDefaultDisplayHiddenColumns())
				.defaultPrimaryFilters(metaData.getDefaultPrimaryFilters())
				.defaultKeywordFilters(metaData.getDefaultKeywordFilters())
				.defaultColumnMasked(metaData.getDefaultColumnMasked())
				.analyticsMode(metaData.getAnalyticsMode())
				.schemaFilePath(metaData.getSchemaFilePath())
				.mcEnrichmentFilePath(metaData.getMcEnrichmentFilePath())
				.uniqueRecordIdentifier(metaData.getUniqueRecordIdentifier())
				.recordObservationDateTime(metaData.getRecordObservationDateTime())
				.surveillanceCaseType(metaData.getSurveillanceCaseType())
				.orgUnit(metaData.getOrgUnit())
				.columnSchema(convertStringToJavaObject(metaData.getColumnSchema()))
				.build();
		
	}
	public String getJsonStringFromObject(Object object) {
		String jsonString  =null;
		try {
			 
			ObjectMapper mapper = new ObjectMapper();
			mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			jsonString = mapper.writeValueAsString(object);

		} catch (Exception e) {
			log.error("Exception {}",e.getMessage());
			return null;
		}

		return jsonString;
	}
	
	public Object convertStringToJavaObject(Object object) {
			Object dataWithRule =null;
		try {
			 
			ObjectMapper mapper = new ObjectMapper();
			mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			  dataWithRule = mapper.readValue(getJsonStringFromObject(object), new TypeReference<List<Object>>() {});

		} catch (Exception e) {
			log.error("Exception {}",e.getMessage());
			return null;
		}

		return dataWithRule;
	}
	
}
