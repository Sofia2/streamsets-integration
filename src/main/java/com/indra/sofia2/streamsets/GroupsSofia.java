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
public enum GroupsSofia implements Label {
  SOFIA2("Sofia2"),
  SOFIA2O("Origen"),
  SOFIA2D("Destino");

  private final String label;

  private GroupsSofia(String label) {
    this.label = label;
  }

  /** {@inheritDoc} */
  @Override
  public String getLabel() {
    return this.label;
  }
}