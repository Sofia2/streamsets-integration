/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  ERROR_00("Incorrect configuration: {}"),
  ERROR_01("Error escribiendo el registro: {}, el error generado es {}"),
  ERROR_02("Error leyendo el fichero: {}"),
  ERROR_03("Error IO: {} al recuperar el Libro Excel del fichero: {}"),
  ERROR_04("Error IO: {} al configurar el acceso a Hadoop: {}")

  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}
