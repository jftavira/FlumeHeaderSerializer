package com.produban.flume;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

/**
 * FlumeHeaderSerializer an implementation of a Flume serializer that adds headers to the written Event 
 * 
 * @author      Juan F Tavira <juftavira (at) produban (dot)com>
 * @version     1.0
 * @since       2015-04-13
 */

public class HeaderSerializer implements EventSerializer, Configurable {
	
	
	private static final Logger logger = LoggerFactory.getLogger(HeaderSerializer.class);
	
	private boolean appendNewline = false;
	private OutputStream out;
	
	String[] headersToWrite;
	Integer numHeaders=0;
	String headersGF="";
	String headersGF_O="";
	String headersGF_S="";
	String headersGF_C="";
	String headersCF="";
	Boolean hasCF=false;
	Boolean hasGF=false;
	Boolean allHeaders=false;

	
	/**
	 * HeaderSerializer constructor
	 *
	 * Creates the object
	 *
	 * @param out Output stream where events will be written
	 * @param contect Configuration for the object
	 * @param object Reference object if any
	 * 
	 * @return Nothing
	 * 
	 */
	public HeaderSerializer(OutputStream out, Context context, Object object) {
		
		logger.trace("HeaderSerializer Constructor");
		
		// Object creation
		this.appendNewline = context.getBoolean("appendNewline", Boolean.valueOf(true)).booleanValue();
		this.out = out;
		
		this.configure(context);
	}

	
	/**
	 * configure reads context and prepares all the configuration
	 * 
	 * Will treat the different options: General Format, Composite Format, etc  
	 *
	 * @param context All the configuration for this object read from configuration file
	 * 
	 * @return void
	 * 
	 */
	@Override
	public void configure(Context context) {
		String headers = "";

		logger.info("Configuring");
		
		// Initialize with defaults
		headersGF = HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_DEFAULT;
		headersGF_O = HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_ENCLOSURE_O_DEFAULT;
		headersGF_S = HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_ENCLOSURE_S_DEFAULT;
		headersGF_C = HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_ENCLOSURE_C_DEFAULT;
		
		// Get header List		
		numHeaders=0;		
		logger.info("Reading header list");
		try {
			headers = context.getString(HeaderSerializerConfigurationConstants.HEADERS);
			logger.info("headers= "+headers);
			
			// Fist check for "*" meaning all headers in GF
			if (StringUtils.equals(StringUtils.trim(headers),"*")){
				allHeaders=true;
			}
			else if ( headers!=null && !headers.equals("")){
				allHeaders=false;
				headersToWrite =headers.split(" ");
				numHeaders = headersToWrite.length;
				logger.info("numHeaders= "+numHeaders);
			}
		}
		catch (Exception e) {
			logger.error("EXCEPTION reading header list");
			logger.error(e.toString());
		}
		
		
		// Get header general format
		logger.info("Reading headers General Format");
		try {
			headersGF = context.getString(HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT);
			logger.trace("headersGF= " + headersGF);
			headersGF_O = context.getString(HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_ENCLOSURE_O);
			logger.trace("headersGF_O= " +  headersGF_O);
			headersGF_S = context.getString(HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_ENCLOSURE_S);
			logger.trace("headersGF_O= " +  headersGF_S);
			headersGF_C = context.getString(HeaderSerializerConfigurationConstants.HEADERS_GENERAL_FORMAT_ENCLOSURE_C);
			logger.trace("headersGF_C= " + headersGF_C);
			
			}
		catch (Exception e) {
			logger.error("EXCEPTION reading headers General Format");
			logger.error(e.toString());
			
			}
		finally {
			hasGF=!(StringUtils.isEmpty(headersGF));
			if (StringUtils.isEmpty(headersGF_O)) headersGF_O="";
			if (StringUtils.isEmpty(headersGF_S)) headersGF_S="";
			if (StringUtils.isEmpty(headersGF_C)) headersGF_C="";
		}
	 
		
		
		// Header composite format		
		logger.info("Reading headers Composite Format");
		try {
			headersCF = context
					.getString(HeaderSerializerConfigurationConstants.HEADERS_COMPOSITE_FORMAT);
			logger.trace("headersCF:"+headersCF);
			
			}
		catch (Exception e) {
			logger.error("EXCEPTION reading headers Composite Format");
			logger.error(e.toString());
			headersCF = "";	
			}
		finally	{
			hasCF = !StringUtils.isEmpty(headersCF);
			}
		
		logger.info("End of Configuring");
		
		}

	
	/**
	 * afterCreate Any action required to do after creating the object
	 * 
	 * Nothing currently
	 *
	 * @param nothing
	 * 
	 * @return void
	 * 
	 */
	@Override
	public void afterCreate() throws IOException {

		logger.debug("blank function");
		
	}

	/**
	 * afterReopen Any action required to do after reopening the stream
	 * 
	 * Nothing currently
	 *
	 * @param nothing
	 * 
	 * @return void
	 * 
	 */
	@Override
	public void afterReopen() throws IOException {

		logger.debug("blank function");
		
	}

	
	/**
	 * beforeClose Any action required to do before closing the stream
	 * 
	 * Nothing currently
	 *
	 * @param nothing
	 * 
	 * @return void
	 * 
	 */
	@Override
	public void beforeClose() throws IOException {

		logger.debug("blank function");
		
	}

	
	/**
	 * flush Any action required to do while flushing the stream
	 * 
	 * Nothing currently
	 *
	 * @param nothing
	 * 
	 * @return void
	 * 
	 */
	@Override
	public void flush() throws IOException {

		logger.debug("blank function");
	}

	
	/**
	 * supportsReopen If the object supports reopening the stream
	 * 
	 * Nothing currently
	 *
	 * @param nothing
	 * 
	 * @return false always for now
	 * 
	 */
	@Override
	public boolean supportsReopen() {

		return false;
	}
	
	
	/**
	 * formatHeaders Will return formatted strings with configured headers
	 * 
	 * Will choose between General Format and Composite Format (this takes precendence) and format the readers
	 *
	 * @param headers String map of header-value of the headers for the event
	 * 
	 * @return String Formatted string
	 * 
	 */
	private String formatHeaders(Map<String, String> headers) {

		Integer i = 1;
		String curHeader;
		String curValue;
		String finalHeaderStr = "";
		String localFormat;
		
		logger.debug(".formatHeaders");
		// Check which format will be used
		if (hasCF && numHeaders>0) {
			
			// Custom format
			logger.trace(".hasCF");
			finalHeaderStr = headersCF;
			logger.trace(".hasCF:finalHeaderStr= "+finalHeaderStr);
			
			curHeader ="";

			// Iterate all headers, replace %header with header and $header with header value
			for (i=0;i<numHeaders;i++)
			{
				logger.trace(".hasCF:iterate headers, i= "+i);
				try {
					
					curHeader=headersToWrite[i];
					
					logger.trace(".hasCF:curValue= "+curHeader);
					logger.trace(".hasCF:finalHeaderStr= "+finalHeaderStr);
					
					finalHeaderStr=StringUtils.replace(finalHeaderStr, "%"+curHeader, curHeader, -1);
					curValue=headers.get(curHeader);
					
					logger.trace(".hasCF:curValue= "+curValue);
					logger.trace(".hasCF:finalHeaderStr= "+finalHeaderStr);
					
					finalHeaderStr=StringUtils.replace(finalHeaderStr, "$"+curHeader, curValue, -1); 
					logger.trace(".hasCF:finalHeaderStr= "+finalHeaderStr);
				  }
				  catch (Exception e){
					//header not found
					logger.error(".hasCF: " + curHeader + "not found in event");
				}	
			}
		}
		
		else if (allHeaders || (hasGF && numHeaders>0)) {
			// General format
			logger.debug(".hasGF");
			// Star with opening enclosure
			finalHeaderStr =headersGF_O;
			logger.debug("finalHeaderStr= "+finalHeaderStr);
			
			if (allHeaders){
				logger.debug(".hasGF.allHeaders");
				curHeader="";
				
				i=0;
				for (Map.Entry<String, String> entry : headers.entrySet())
				{
					i++;
					localFormat= headersGF;
					try {
						curHeader=entry.getKey();
						logger.trace(".hasGF.allHeaders:curHeader= "+curHeader);
						curValue=entry.getValue();
						logger.trace(".hasGF.allHeaders:curValue= "+curValue);
						// {%H=%V}
						logger.trace(".hasGF.allHeaders:localFormat= "+ localFormat);
						localFormat=StringUtils.replace(localFormat,"%H", curHeader,-1);
						logger.trace(".hasGF.allHeaders:localFormat= "+ localFormat);
						localFormat=StringUtils.replace(localFormat,"%V", curValue,-1);
						logger.trace(".hasGF.allHeaders:localFormat= "+localFormat);
						localFormat=localFormat+headersGF_S;
						logger.trace(".hasGF.allHeaders:localFormat= "+localFormat);

					}
					catch (Exception e){
						//header not found
						logger.error(".hasGF.allHeaders: error processing header/value #"+ i + " from the event");
					}
					finalHeaderStr = finalHeaderStr + localFormat;
					logger.debug(".hasGF.allHeaders: finalHeaderStr= "+finalHeaderStr);
				}
			}
			else {
				// Iterate all headers, 
				for (i=0;i<numHeaders;i++)
				{
					//locate current header to write
					curHeader=headersToWrite[i];
					logger.trace(".hasGF:curHeader= "+curHeader);
					
					localFormat= headersGF;
					
					try {
						//get value for current header
						curValue=headers.get(curHeader);
						logger.trace(".hasGF:curValue= "+curValue);
						// {%H=%V}
						logger.trace(".hasGF:localFormat= "+ localFormat);
						localFormat=StringUtils.replace(localFormat,"%H", curHeader,-1);
						logger.trace(".hasGF:localFormat= "+ localFormat);
						localFormat=StringUtils.replace(localFormat,"%V", curValue,-1);
						logger.trace(".hasGF:localFormat= "+localFormat);
						localFormat=localFormat+headersGF_S;
						logger.trace(".hasGF:localFormat= "+localFormat);
					}
					catch (Exception e){
						//header not found
						logger.error(".hasGF: " + curHeader + "not found in event");
						}
					
					finalHeaderStr = finalHeaderStr + localFormat;
					logger.debug(".hasGF: finalHeaderStr= "+finalHeaderStr);
				}
			}
			// Remove trailing separator
			finalHeaderStr=StringUtils.removeEnd(finalHeaderStr,headersGF_S);

			// Add closing enclosure			
			finalHeaderStr = finalHeaderStr +headersGF_C;
		}
		
		logger.debug("finalHeaderStr= "+finalHeaderStr);
		return finalHeaderStr;
	}

	/**
	 * write All the actions required to write event and headers
	 * 
	 * Includes formatting
	 *
	 * @param Event To be written, it includes the headers
	 * 
	 * @return void
	 * 
	 */
	@Override
	public void write(Event arg0) throws IOException {
		//String newBody;
		
		String formattedHeaders="";
		
		//Get the headers formatted
		formattedHeaders =formatHeaders(arg0.getHeaders());
		logger.trace("formattedHeaders= "+formattedHeaders);
		//Write the headers
		this.out.write(formattedHeaders.getBytes());
		//Write event
		this.out.write (arg0.getBody());
		
		if (this.appendNewline) {
			this.out.write(10);
		}
		
	}

	/**
	 * Builder class Mandatory generic builder to create Event Serializer class
	 * 
	 * Implements EventSerializer.Builder
	 *
	 * @param context With all relevant configuration
	 * @param out Stream where to write events
	 * 
	 * @return void
	 * 
	 */
	public static class Builder implements EventSerializer.Builder 
		{
		public EventSerializer build(Context context, OutputStream out)
			{
				logger.info("Constructor Builder");
				HeaderSerializer s = new HeaderSerializer(out, context, null);
				return s;
			}
		}
}

/**
 * Configuration read from flume:
 * 
 * serializer.headers : header list to be printed at the begining of the event, blank separated. No default. * will write all the headers in the event
 * serializer.headers_gf : general format, for each header named in .headers property %H will be replaced with header name, %V will be replaced with header value. %H=%V will become filename=/opt/data/log.txt
 * serializer.headers_gf_o : general format, opening character to be added one single time to the whole headers string
 * serializer.headers_gf_s : general format, separator character to be added separating different pairs of header=value
 * serializer.headers_gf_c : general format, closing character to be added one single time to the whole headers string
 * serializer.headers_cf : composite format, will print all the header-values named here with the specific format, not the general, [ %test_header =  $test_header ] will become [ filename =  /opt/data/log.txt ], middle blanks are important
 * 
 * In case of general format and composite format both appears composite format will take precedence.
 * 
 */