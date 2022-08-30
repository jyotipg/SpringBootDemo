package com.haud.svalinn.lib.ldap.util;

import static org.springframework.ldap.query.LdapQueryBuilder.query;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.naming.CommunicationException;
import javax.naming.Name;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.NoPermissionException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.ldap.AuthenticationException;
import org.springframework.ldap.NameAlreadyBoundException;
import org.springframework.ldap.NameNotFoundException;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.support.LdapNameBuilder;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.context.annotation.Configuration;
import com.haud.svalinn.lib.ldap.exceptions.LdapApiException;

@Configuration
public class LdapConfig {
	
	private LdapTemplate ldapTemplate;

	private String dnClass;
	private String dnAttribute;
	private String userName;
	private String groupName;
	private String memberClass;
	private String memberAttribute;
	private String findBy;
	private String ldapUrl;
	private String userDn;
	private String password;
	private String setBase;
	private String searchOn;
	private String searchByAsterisk;
	private List<String> restrictedAttributesWhileUpdate;
	private List<String> restrictedSuperUsers;
	public static final String OBJECT_CLASS = "objectclass";
	public static final String SAM_ACCOUNT_NAME = "sAMAccountName";
	public static final String DISPLAY_NAME = "displayName";
	public static final String FIRST_NAME = "givenName";
	public static final String USER_PASSWORD = "userPassword";
	public static final String SECOND_NAME = "sn";	
	public static final String USER_NAME = "cn";

	 

	@Autowired
	public LdapConfig(String ldapUrl, String userDn, String password, String setBase,
			List<String> restrictedAttributesWhileUpdate, List<String> restrictedSuperUsers) {
		this.ldapUrl = ldapUrl;
		this.userDn = userDn;
		this.password = password;
		this.setBase = setBase;
		this.restrictedAttributesWhileUpdate = restrictedAttributesWhileUpdate;
		this.restrictedSuperUsers=restrictedSuperUsers;
	}

	@PostConstruct
	public void init() throws Exception {
		DefaultSpringSecurityContextSource contextSource = new DefaultSpringSecurityContextSource(ldapUrl);
		contextSource.setBase(setBase);
		contextSource.setUserDn(userDn);
		contextSource.setPassword(password);
		contextSource.setReferral("follow");
		contextSource.afterPropertiesSet();
		this.ldapTemplate = new LdapTemplate(contextSource);
		this.ldapTemplate.setIgnorePartialResultException(true);
		this.ldapTemplate.afterPropertiesSet();	 

	}

	 

 	// This is useful for you will get user and groups
	public  String  mapUserAndGroup(String userName,String groupName,String dnAttribute,String memberClass,String memberAttribute) throws CommunicationException,ConnectException,NoPermissionException{
	 
			String result=null;
			Name userDnBuilder = buildDn(dnAttribute, userName);
			Name groupDnBuilder = buildDn(dnAttribute, groupName);
			String searchCriteria = dnAttribute + "=" + userName;
			DirContextOperations groupctx =null;
			DirContextOperations userctx=null;
			try
			{
				groupctx = ldapTemplate.lookupContext(groupDnBuilder);
				
			} catch (AuthenticationException | NameNotFoundException e) {
				groupctx =null;
			}
			try
			{ 
				userctx = ldapTemplate.lookupContext(userDnBuilder);
			} catch (AuthenticationException  | NameNotFoundException e) {
				 
				userctx =null;
			}
			
			if (groupctx==null && userctx==null)
			{
				result=" User " + userName + " and  Group " + groupName + " not found";
				
			}
			else if (groupctx==null)
			{
				result=" Group " + groupName + " not found";
				
				
			}
			else if (userctx==null)
			{
				result=" User " + userName +" not found";				
			}
			else
			{			
			 try {
				 List<Map<String, Object>> attributeList = getAttributes(userDnBuilder, searchCriteria);			
					groupctx.addAttributeValue(memberClass, attributeList.get(0).get(memberAttribute));
					ldapTemplate.modifyAttributes(groupctx);
			} catch (Exception e) {
				result=" User " + userName +" not found";
			}
				
			}
			return result;
		 
	}
	 
	private Name buildDn(String dnClassName) {

		return LdapNameBuilder.newInstance().add(dnClassName, dnAttribute).build();
	}

	private Name buildDn(String dnClassName, String dnAttributeName) {

		return LdapNameBuilder.newInstance().add(dnClassName, dnAttributeName).build();
	}

	public void mapToContext(Map<String, String> attributes, DirContextOperations context) throws CommunicationException,ConnectException,NoPermissionException{
		for (Entry<String, String> attribute : attributes.entrySet()) {
			if (attribute.getKey().equalsIgnoreCase("USER_ACCOUNT_CONTROL"))
			{
				context.setAttributeValue(attribute.getKey(), Double.valueOf(attribute.getValue()));
			}
			else
			{
				context.setAttributeValue(attribute.getKey(), attribute.getValue());
			}
		}
	}

 

	public List<Map<String, Object>> find(String operation,String findBy) throws CommunicationException,ConnectException,NoPermissionException{
		try {
			String searchCriteria = dnAttribute + "=" + findBy;
			Name searachDn = buildDn(dnAttribute, findBy);
			return getAttributes(searachDn, searchCriteria);
		} 
		catch (AuthenticationException  e)
		{
			throw new LdapApiException("Internal Service Error", HttpStatus.BAD_REQUEST,e);
		}
		catch( NameNotFoundException e) {
			throw new LdapApiException(operation+" " + findBy + " not found", HttpStatus.NOT_FOUND,e);
		} catch (Exception e) {
			throw new LdapApiException(e.getMessage(), HttpStatus.BAD_REQUEST,e);
		}

	}

	 
 
	public List<Map<String, Object>> findMembers(String searchByAsterisk,String findBy,String searchOn) throws CommunicationException,ConnectException,NoPermissionException {
		try {
			if (searchOn!=null)
			{
			return getAttribute(searchOn+"="+findBy+"*");
			}
			else if (searchByAsterisk!=null)
			{
				return getAttribute("*"+findBy+"*");
			}
			else
			{
				return getAttribute(findBy);
			}
		} catch (AuthenticationException  e) {
			throw new LdapApiException("Internal Service Error", HttpStatus.BAD_REQUEST,e);
		}
		catch (NameNotFoundException e) {
			throw new LdapApiException("find with " + findBy + " not found", HttpStatus.NOT_FOUND,e);
		} catch (Exception e) {
			throw new LdapApiException(e.getMessage(), HttpStatus.BAD_REQUEST,e);
		}

	}
	 
	private List<Map<String, Object>> getAttributes(Name searachDn, String searchCriteria) {
		return ldapTemplate.search(searachDn, searchCriteria, new AttributesMapper<Map<String, Object>>() {
			public Map<String, Object> mapFromAttributes(Attributes attributes) throws NamingException {
				Map<String, Object> map = new HashMap<>();
				NamingEnumeration<? extends Attribute> enumeration = attributes.getAll();
				while (enumeration.hasMoreElements()) {
					Attribute attribute = enumeration.nextElement();
					map.put(attribute.getID(), attributes.get(attribute.getID()).get());
				}
				return map;
			}
		});

	}

	private List<Map<String, Object>> getAttribute(String searchCriteria) {
		
		return ldapTemplate.search( 
				query().where(OBJECT_CLASS).is(dnClass).and(query().where(dnAttribute).like(searchCriteria)),
				new AttributesMapper<Map<String, Object>>() {
					public Map<String, Object> mapFromAttributes(Attributes attributes) throws NamingException {
						Map<String, Object> map = new HashMap<>();
						NamingEnumeration<? extends Attribute> enumeration = attributes.getAll();
						while (enumeration.hasMoreElements()) {
							Attribute attribute = enumeration.nextElement();
							map.put(attribute.getID(), attributes.get(attribute.getID()).get());
						}
						return map;
					}
				});

	}
	
	private List<Map<String, Object>> getAttributeList() {
		return ldapTemplate.search(
				query().where(OBJECT_CLASS).is(dnClass),
				new AttributesMapper<Map<String, Object>>() {
					public Map<String, Object> mapFromAttributes(Attributes attributes) throws NamingException {
						Map<String, Object> map = new HashMap<>();
						NamingEnumeration<? extends Attribute> enumeration = attributes.getAll();
						while (enumeration.hasMoreElements()) {
							Attribute attribute = enumeration.nextElement();
							map.put(attribute.getID(), attributes.get(attribute.getID()).get());
						}
						return map;
					}
				});

	}	
}
