/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum GroupsHDFS implements Label {
	
  HDFS("HDFS"),
  DELIMITED("Delimited");
  
  private final String label;

  private GroupsHDFS(String label) {
    this.label = label;
  }

  /** {@inheritDoc} */
  @Override
  public String getLabel() {
    return this.label;
  }
}