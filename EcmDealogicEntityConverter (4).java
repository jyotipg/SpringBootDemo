package com.hsbc.gbm.surveillance.sdf.trade.processor.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.util.StringUtils;

import com.hsbc.gbm.surveillance.sdf.trade.processor.qualified.response.SchemaFields;

import io.reactivex.Flowable;
import io.reactivex.subscribers.DisposableSubscriber;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EcmDealogicEntityConverter {
	private static EcmDealogic e1 = null;
	public static List<EcmDealogic> convertToEcmDealogicEntity(List<SchemaFields> schemaFields) throws ParseException {
		List<EcmDealogic> objectList = new ArrayList<EcmDealogic>();
		
		Flowable.fromIterable(schemaFields).flatMap(schemaField -> {
			return Flowable.just((SchemaFields) schemaField);
		}).subscribeWith(new DisposableSubscriber<SchemaFields>() {

			@Override
			public void onNext(SchemaFields schemaField) {
				e1 = new EcmDealogic();
				EcmDealogic.UniqueKey uniqueKey = new EcmDealogic.UniqueKey(null, null);
				Flowable.fromIterable(schemaField.getSchemaFields()).subscribe(field -> {
					String columnStandardizedName = field.getColumnStandardizedName();
					String formatedColumnValue = field.getFormatedColumnValue();
					e1 = createUpdateEcmDealogicObject(e1, columnStandardizedName, formatedColumnValue, uniqueKey);
					
				});
				e1.setKey(uniqueKey);
				log.info("Ecm Dealogic Entity {}",e1);
				objectList.add(e1);
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
		return objectList;
	}

	private static EcmDealogic createUpdateEcmDealogicObject(EcmDealogic e1, String columnStandardizedName,
			String formatedColumnValue, EcmDealogic.UniqueKey uniqueKey) throws ParseException {

		if (columnStandardizedName.equals(EcmDealogicFieldConstant.BUSINESSDATE)) {
			e1.setBusinessDate(setDate(formatedColumnValue));

		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.TRADEDATE)) {
			e1.setTradeDate(setDate(formatedColumnValue));
		}

		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALNAME)) {
			e1.setDealName(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALTYPE)) {
			e1.setDealType(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.SECTOR)) {
			e1.setSector(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALREGION)) {
			e1.setDealRegion(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALCURRENCY)) {
			e1.setDealCurrency(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALSIZE)) {
			e1.setDealSize(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.LISTINGMARKET)) {
			e1.setListingMarket(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.INVESTORCODE)) {
			e1.setInvestorCode(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.INVESTORNAME)) {
			e1.setInvestorName(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.HSBCSYNDICATEROLE)) {
			e1.setHsbcSyndicateRole(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALERCODE)) {
			e1.setDealerCode(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.MAXDEMAND)) {
			e1.setMaxDemand(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEMANDENTERED)) {
			e1.setDemandEntered(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEMANDFACTOR)) {
			e1.setDemandFactor(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.LIMITENTERED)) {
			e1.setLimitEntered(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.EFFECTIVEDEMAND)) {
			e1.setEffectiveDemand(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.ORDERDATETIME)) {
			e1.setOrderDateTime(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.ALLOCATIONPRICE)) {
			e1.setAllocationPrice(setBigDecimal(formatedColumnValue));
		}

		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DUPLICATESPLIT)) {
			e1.setDuplicatePrice(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.INCLUDEDUPLICATES)) {
			e1.setIncludeDuplicates(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.TRADERNAME)) {
			e1.setTraderName(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.SURVEILLANCETYPE)) {
			e1.setSurveillanceCaseType(formatedColumnValue);
		}		
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.POTALLOCATIONUNIT)) {
			e1.setPotAllocationUnit(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.POTALLOCATIONAMOUNT)) {
			e1.setPotAllocationAmount(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DELIVERBY)) {
			e1.setDeliverBy(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.OFFERPRICE)) {
			e1.setOfferPrice(setBigDecimal(formatedColumnValue));
		}

		if (columnStandardizedName.equals(EcmDealogicFieldConstant.INVESTORTYPE)) {
			e1.setInvestorType(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.BALANCESHEET)) {
			e1.setBalanceSheet(formatedColumnValue);
		}

		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEALMANAGERID)) {
			e1.setDealManagerID(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.EBBID)) {
			e1.setEBBId(setBigDecimal(formatedColumnValue));
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.UNDERWRITINGCENTERS)) {
			e1.setUnderWritingCenters(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.DEAL_ID)) {
			uniqueKey.setDealId(formatedColumnValue);
		}
		if (columnStandardizedName.equals(EcmDealogicFieldConstant.ORDER_ID)) {
			uniqueKey.setOrderId(formatedColumnValue);
		}
		 
		return e1;
	}

	private static BigDecimal setBigDecimal(String value) {
		BigDecimal bigDecimal = new BigDecimal(Float.valueOf("0.00"));
		if (StringUtils.hasLength(value)) {
			String decimalValue = String.valueOf(value).replaceAll(",", "");
			bigDecimal = new BigDecimal(Float.valueOf(decimalValue)).setScale(2, RoundingMode.HALF_UP);
		}
		return bigDecimal;
	}

	private static Date setDate(String value) {
		if (StringUtils.hasLength(value)) {
			try {
			 return java.sql.Date.valueOf(LocalDate.parse(value, DateTimeFormatter.ofPattern("dd/MMM/yyyy")));
			} catch (Exception e) {
				return null;
			}
		}
		return null;
	}

}