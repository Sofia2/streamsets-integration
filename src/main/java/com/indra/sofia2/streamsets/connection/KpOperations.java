/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.connection;

import com.indra.sofia2.ssap.ssap.body.SSAPBodyReturnMessage;

public interface KpOperations {

	SSAPBodyReturnMessage leave();

	SSAPBodyReturnMessage query(String ontology, String query, String queryType);

	SSAPBodyReturnMessage insert(String message, String ontology, Integer bulk);

}