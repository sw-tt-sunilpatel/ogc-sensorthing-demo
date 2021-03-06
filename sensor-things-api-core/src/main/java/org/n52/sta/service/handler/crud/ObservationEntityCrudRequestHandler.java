/*
 * Copyright (C) 2018-2019 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * If the program is linked with libraries which are licensed under one of
 * the following licenses, the combination of the program with the linked
 * library is not considered a "derivative work" of the program:
 *
 *     - Apache License, version 2.0
 *     - Apache Software License, version 1.0
 *     - GNU Lesser General Public License, version 3
 *     - Mozilla Public License, versions 1.0, 1.1 and 2.0
 *     - Common Development and Distribution License (CDDL), version 1.0
 *
 * Therefore the distribution of the program linked with libraries licensed
 * under the aforementioned licenses, is permitted by the copyright holders
 * if the distribution is compliant with both the GNU General Public
 * License version 2 and the aforementioned licenses.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 */
package org.n52.sta.service.handler.crud;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.server.api.ODataApplicationException;
import org.n52.series.db.beans.DataEntity;
import org.n52.sta.data.service.AbstractSensorThingsEntityService;
import org.n52.sta.data.service.EntityServiceRepository.EntityTypes;
import org.n52.sta.mapping.AbstractMapper;
import org.n52.sta.mapping.ObservationMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ObservationEntityCrudRequestHandler extends AbstractEntityCrudRequestHandler<DataEntity<?>> {

    @Autowired
    private ObservationMapper mapper;

    @Override
    @Transactional
    protected Entity handleCreateEntityRequest(Entity entity) throws ODataApplicationException {
        if (entity != null) {
            DataEntity<?> observation = getEntityService().create(mapper.createEntity(getMapper().checkEntity(entity)));
            Entity obs = mapToEntity(observation);
            return obs;
        }
        return null;
    }

    @Override
    protected Entity handleUpdateEntityRequest(Entity entity, HttpMethod method) throws ODataApplicationException {
        if (entity != null) {
            DataEntity<?> datastream = getEntityService().update(mapper.createEntity(entity), method);
            return mapToEntity(datastream);
        }
        return null;
    }

    @Override
    protected void handleDeleteEntityRequest(String id) throws ODataApplicationException {
        getEntityService().delete(id.replace("\'", ""));
    }

    @Override
    protected AbstractMapper<DataEntity<?>> getMapper() {
        return mapper;
    }

    @SuppressWarnings("unchecked")
    private AbstractSensorThingsEntityService<?, DataEntity<?>> getEntityService() {
        return (AbstractSensorThingsEntityService<?, DataEntity<?>>) getEntityService(EntityTypes.Observation);
    }
}
