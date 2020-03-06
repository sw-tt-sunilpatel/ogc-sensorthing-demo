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
package org.n52.sta.mapping;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.n52.series.db.beans.sta.HistoricalLocationEntity;
import org.n52.series.db.beans.sta.LocationEntity;
import org.n52.shetland.ogc.gml.time.Time;
import org.n52.shetland.ogc.gml.time.TimeInstant;
import org.n52.shetland.ogc.gml.time.TimePeriod;
import org.n52.sta.edm.provider.SensorThingsEdmConstants;
import org.n52.sta.edm.provider.entities.AbstractSensorThingsEntityProvider;
import org.n52.sta.edm.provider.entities.HistoricalLocationEntityProvider;
import org.n52.sta.edm.provider.entities.LocationEntityProvider;
import org.n52.sta.edm.provider.entities.ThingEntityProvider;
import org.n52.sta.utils.EntityCreationHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

//import org.n52.sta.utils.EntityAnnotator;

/**
 * @author <a href="mailto:j.speckamp@52north.org">Jan Speckamp</a>
 */
@Component
public class HistoricalLocationMapper extends AbstractMapper<HistoricalLocationEntity> {

    private final EntityCreationHelper entityCreationHelper;
    private final LocationMapper locationMapper;
    private final ThingMapper thingMapper;

    @Autowired
    public HistoricalLocationMapper(EntityCreationHelper entityCreationHelper,
                                    @Lazy LocationMapper locationMapper,
                                    @Lazy ThingMapper thingMapper) {
        this.entityCreationHelper = entityCreationHelper;
        this.locationMapper = locationMapper;
        this.thingMapper = thingMapper;
    }

    public Entity createEntity(HistoricalLocationEntity location) {
        Entity entity = new Entity();

        entity.addProperty(new Property(
                null,
                AbstractSensorThingsEntityProvider.PROP_ID,
                ValueType.PRIMITIVE,
                location.getIdentifier()));
        entity.addProperty(new Property(
                null,
                AbstractSensorThingsEntityProvider.PROP_TIME,
                ValueType.PRIMITIVE,
                location.getTime().getTime()));

        entity.setType(HistoricalLocationEntityProvider.ET_HISTORICAL_LOCATION_FQN.getFullQualifiedNameAsString());
        entity.setId(entityCreationHelper.createId(
                entity,
                HistoricalLocationEntityProvider.ES_HISTORICAL_LOCATIONS_NAME,
                AbstractSensorThingsEntityProvider.PROP_ID));

        return entity;

    }

    @Override
    public HistoricalLocationEntity createEntity(Entity entity) {
        HistoricalLocationEntity historicalLocation = new HistoricalLocationEntity();
        setIdentifier(historicalLocation, entity);
        addTime(historicalLocation, entity);
        addThing(historicalLocation, entity);
        addLocations(historicalLocation, entity);
        return historicalLocation;
    }

    private Time createTime(HistoricalLocationEntity location) {
        return createTime(createDateTime(location.getTime()));
    }

    @Override
    public HistoricalLocationEntity merge(HistoricalLocationEntity existing, HistoricalLocationEntity toMerge) {
        if (toMerge.getTime() != null) {
            existing.setTime(toMerge.getTime());
        }
        return existing;
    }

    protected void setIdentifier(HistoricalLocationEntity idEntity, Entity entity) {
        String rawId;
        if (checkProperty(entity, SensorThingsEdmConstants.ID)) {
            rawId = getPropertyValue(entity, SensorThingsEdmConstants.ID).toString();
        } else if (checkProperty(entity, SensorThingsEdmConstants.ID_ANNOTATION)) {
            rawId = getPropertyValue(entity, SensorThingsEdmConstants.ID_ANNOTATION).toString();
        } else {
            rawId = UUID.randomUUID().toString();
        }

        // URLEncode identifier.
        try {
            idEntity.setIdentifier(URLEncoder.encode(rawId, "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void addTime(HistoricalLocationEntity historicalLocation, Entity entity) {
        if (checkProperty(entity, AbstractSensorThingsEntityProvider.PROP_TIME)) {
            Time time = parseTime(getPropertyValue(entity, AbstractSensorThingsEntityProvider.PROP_TIME));
            if (time instanceof TimeInstant) {
                historicalLocation.setTime(((TimeInstant) time).getValue().toDate());
            } else if (time instanceof TimePeriod) {
                historicalLocation.setTime(((TimePeriod) time).getEnd().toDate());
            }
        }
    }

    private void addThing(HistoricalLocationEntity historicalLocation, Entity entity) {
        if (checkNavigationLink(entity, ThingEntityProvider.ET_THING_NAME)) {
            historicalLocation.setThing(
                    thingMapper.createEntity(entity.getNavigationLink(ThingEntityProvider.ET_THING_NAME)
                                                   .getInlineEntity()));
        }
    }

    private void addLocations(HistoricalLocationEntity historicalLocation, Entity entity) {
        if (checkNavigationLink(entity, LocationEntityProvider.ES_LOCATIONS_NAME)) {
            Set<LocationEntity> locations = new LinkedHashSet<>();
            Iterator<Entity> iterator = entity.getNavigationLink(LocationEntityProvider.ES_LOCATIONS_NAME)
                                              .getInlineEntitySet().iterator();
            while (iterator.hasNext()) {
                locations.add(locationMapper.createEntity((Entity) iterator.next()));
            }
            historicalLocation.setLocations(locations);
        }
    }

    @Override
    public Entity checkEntity(Entity entity) throws ODataApplicationException {
        checkPropertyValidity(AbstractSensorThingsEntityProvider.PROP_TIME, entity);
        return entity;
    }
}
