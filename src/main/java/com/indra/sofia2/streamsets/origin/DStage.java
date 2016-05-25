/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import com.streamsets.pipeline.api.Stage;

import java.util.List;

public abstract class DStage<C extends Stage.Context> implements Stage<C> {
  private Stage<C> stage;

  public Stage<C> getStage() {
    return stage;
  }

  abstract Stage<C> createStage();

  @Override
  public final List<ConfigIssue> init(Info info, C context) {
    if(stage == null) {
      stage = createStage();
    }
    return stage.init(info, context);
  }

  @Override
  public final void destroy() {
    stage.destroy();
  }

}
