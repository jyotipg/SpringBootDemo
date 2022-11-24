package com.hsbc.gbm.surveillance.sdf.trade.processor.convertor;

import java.util.List;

import org.springframework.stereotype.Service;

import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.SchemaFields;
import com.hsbc.gbm.surveillance.sdf.trade.processor.service.EcmDealogicService;
import com.hsbc.gbm.surveillance.sdf.trade.processor.service.EcmMetaDataService;
import com.hsbc.gbm.surveillance.sdf.trade.processor.utils.EcmDealogic;
import com.hsbc.gbm.surveillance.sdf.trade.processor.utils.EcmDealogicEntityConverter;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Data
@Builder
@Slf4j
public class ServiceFactory  {
 
 
	private final DataStandardization dataStandardization;
	
	private final EcmMetaDataService ecmMetaDataService;
	
	private final EcmDealogicService ecmDealogicService;
	
	
	public void dataStandardizationAndSaving(String jsonString, String businessDate ,String reference)
	{	 
		List<SchemaFields> SchemaFieldsRows=dataStandardization.jsonObjectProcessor(jsonString, businessDate);
		
		if (reference.contains( "EcmDealogic"))
		{
			List<EcmDealogic> data =EcmDealogicEntityConverter.convertToEcmDealogicEntity(SchemaFieldsRows);
			log.info("entity {}",data);
			log.info("{} Data successfully saved ",reference);
			ecmDealogicService.saveAllAndFlush(data);
		}
		
				
	}
	
} 
