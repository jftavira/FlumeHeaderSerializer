/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.produban.flume;

public final class HeaderSerializerConfigurationConstants {


	/* Property to define headers to write */
	public static final String HEADERS = "headers";
	/* General format for headers */
	public static final String HEADERS_GENERAL_FORMAT = "headers_gf";
	/*
	 * General format for headers	  
	 * For all the headers listed in "headers"
	 * Example:
	 * agent.sinks.sinkhdfs.headers_gf= %H=%V;
	 * agent.sinks.sinkhdfs.headers_gf_o= {;
	 * agent.sinks.sinkhdfs.headers_gf_c= };
	 * 
	 * will write:
	 * { filename=/opt/app/logs/app.log;}
	 * or
	 * { filename=/opt/app/logs/app.log; hostname=mymachime.domain.com;}
	 * 
	 * TAKE CARE OF BLANKS!
	 */
	public static final String HEADERS_GENERAL_FORMAT_DEFAULT = "%H=%V";
	
	public static final String HEADERS_GENERAL_FORMAT_ENCLOSURE_O = "headers_gf_o";
	public static final String HEADERS_GENERAL_FORMAT_ENCLOSURE_O_DEFAULT = "{";
	
	public static final String HEADERS_GENERAL_FORMAT_ENCLOSURE_S = "headers_gf_s";
	public static final String HEADERS_GENERAL_FORMAT_ENCLOSURE_S_DEFAULT = ";";
	
	public static final String HEADERS_GENERAL_FORMAT_ENCLOSURE_C = "headers_gf_c";
	public static final String HEADERS_GENERAL_FORMAT_ENCLOSURE_C_DEFAULT = "}";
	
	
	/*
	 * Composite format for headers 
	 * Takes precedence over HEADERS_GENERAL_FORMAT in case both exist
	 * There is no default for composite format, if blank or missing GF apply 
	 * HCF placeholders are: %header_name $header_value
	 * %% and $$ are scaped to "%" and "$"
	 * Missing %header_names will not be written 
	 * Example:
	 * agent.sinks.sinkhdfs.headers_cf = { %filename = $filename; %hostname = $hostname }
	 * 
	 * will write:
	 * { filename=/opt/app/logs/app.log; hostname=mymachime.domain.com }
	 */
	
	public static final String HEADERS_COMPOSITE_FORMAT = "headers_cf";
	
	
}
