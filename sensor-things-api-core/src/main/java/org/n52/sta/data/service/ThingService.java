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
package org.n52.sta.data.service;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.joda.time.DateTime;
import org.n52.series.db.beans.PlatformEntity;
import org.n52.series.db.beans.sta.DatastreamEntity;
import org.n52.series.db.beans.sta.HistoricalLocationEntity;
import org.n52.series.db.beans.sta.LocationEntity;
import org.n52.sta.data.query.ThingQuerySpecifications;
import org.n52.sta.data.repositories.ThingRepository;
import org.n52.sta.data.service.EntityServiceRepository.EntityTypes;
import org.n52.sta.edm.provider.entities.DatastreamEntityProvider;
import org.n52.sta.edm.provider.entities.HistoricalLocationEntityProvider;
import org.n52.sta.edm.provider.entities.LocationEntityProvider;
import org.n52.sta.mapping.ThingMapper;
import org.n52.sta.service.query.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:s.drost@52north.org">Sebastian Drost</a>
 */
@Component
@DependsOn({"springApplicationContext"})
public class ThingService extends AbstractSensorThingsEntityService<ThingRepository, PlatformEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ThingService.class);
    private static final ThingQuerySpecifications tQS = new ThingQuerySpecifications();

    private final String IOT_LOCATION = "iot.Location";

    private ThingMapper mapper;

    public ThingService(ThingRepository repository, ThingMapper mapper) {
        super(repository);
        this.mapper = mapper;
    }

    @Override
    public EntityTypes getType() {
        return EntityTypes.Thing;
    }

    @Override
    public EntityCollection getEntityCollection(QueryOptions queryOptions) throws ODataApplicationException {
        EntityCollection retEntitySet = new EntityCollection();
        Specification<PlatformEntity> filter = getFilterPredicate(PlatformEntity.class, queryOptions);
        getRepository().findAll(filter, createPageableRequest(queryOptions))
                       .forEach(t -> retEntitySet.getEntities().add(mapper.createEntity(t)));
        return retEntitySet;
    }

    @Override
    public Entity getEntity(String identifier) {
        Optional<PlatformEntity> entity = getRepository().findByIdentifier(identifier);
        return entity.isPresent() ? mapper.createEntity(entity.get()) : null;
    }

    @Override
    public EntityCollection getRelatedEntityCollection(String sourceIdentifier,
                                                       EdmEntityType sourceEntityType,
                                                       QueryOptions queryOptions) {
        Specification<PlatformEntity> filter = tQS.withRelatedLocationIdentifier(sourceIdentifier);

        filter = filter.and(getFilterPredicate(PlatformEntity.class, queryOptions));
        Iterable<PlatformEntity> things = getRepository().findAll(filter, createPageableRequest(queryOptions));

        EntityCollection retEntitySet = new EntityCollection();
        things.forEach(t -> retEntitySet.getEntities().add(mapper.createEntity(t)));
        return retEntitySet;
    }

    @Override
    public long getRelatedEntityCollectionCount(String sourceIdentifier, EdmEntityType sourceEntityType) {
        return getRepository().count(tQS.withRelatedLocationIdentifier(sourceIdentifier));
    }

    @Override
    public boolean existsEntity(String identifier) {
        return getRepository().existsByIdentifier(identifier);
    }

    @Override
    public boolean existsRelatedEntity(String sourceId, EdmEntityType sourceEntityType) {
        return this.existsRelatedEntity(sourceId, sourceEntityType, null);
    }

    @Override
    public boolean existsRelatedEntity(String sourceIdentifier,
                                       EdmEntityType sourceEntityType,
                                       String targetIdentifier) {
        switch (sourceEntityType.getFullQualifiedName().getFullQualifiedNameAsString()) {
            case IOT_LOCATION: {
                Specification<PlatformEntity> filter = tQS.withRelatedLocationIdentifier(sourceIdentifier);
                if (targetIdentifier != null) {
                    filter = filter.and(tQS.withIdentifier(targetIdentifier));
                }
                return getRepository().count(filter) > 0;
            }
            default:
                return false;
        }
    }

    @Override
    public Optional<String> getIdForRelatedEntity(String sourceId, EdmEntityType sourceEntityType) {
        return this.getIdForRelatedEntity(sourceId, sourceEntityType, null);
    }

    @Override
    public Optional<String> getIdForRelatedEntity(String sourceId, EdmEntityType sourceEntityType, String targetId) {
        Optional<PlatformEntity> thing = this.getRelatedEntityRaw(sourceId, sourceEntityType, targetId);
        return thing.map(platformEntity -> Optional.of(platformEntity.getIdentifier())).orElseGet(Optional::empty);
    }

    @Override
    public Entity getRelatedEntity(String sourceId, EdmEntityType sourceEntityType) {
        return this.getRelatedEntity(sourceId, sourceEntityType, null);
    }

    @Override
    public Entity getRelatedEntity(String sourceId, EdmEntityType sourceEntityType, String targetId) {
        Optional<PlatformEntity> thing = this.getRelatedEntityRaw(sourceId, sourceEntityType, targetId);
        return thing.map(platformEntity -> mapper.createEntity(platformEntity)).orElse(null);
    }

    /**
     * Retrieves Thing Entity with Relation to sourceEntity from Database.
     * Returns empty if Thing is not found or Entities are not related.
     *
     * @param sourceIdentifier Id of the Source Entity
     * @param sourceEntityType Type of the Source Entity
     * @param targetIdentifier Id of the Thing to be retrieved
     * @return Optional&lt;PlatformEntity&gt; Requested Entity
     */
    private Optional<PlatformEntity> getRelatedEntityRaw(String sourceIdentifier,
                                                         EdmEntityType sourceEntityType,
                                                         String targetIdentifier) {
        Specification<PlatformEntity> filter;
        switch (sourceEntityType.getFullQualifiedName().getFullQualifiedNameAsString()) {
            case "iot.HistoricalLocation": {
                filter = tQS.withRelatedHistoricalLocationIdentifier(sourceIdentifier);
                break;
            }
            case "iot.Datastream": {
                filter = tQS.withRelatedDatastreamIdentifier(sourceIdentifier);
                break;
            }
            case IOT_LOCATION: {
                filter = tQS.withRelatedLocationIdentifier(sourceIdentifier);
                break;
            }
            default:
                return Optional.empty();
        }

        if (targetIdentifier != null) {
            filter = filter.and(tQS.withIdentifier(targetIdentifier));
        }
        return getRepository().findOne(filter);
    }

    @Override
    public long getCount(QueryOptions queryOptions) throws ODataApplicationException {
        return getRepository().count(getFilterPredicate(PlatformEntity.class, queryOptions));
    }

    @Override
    public PlatformEntity create(PlatformEntity newThing) throws ODataApplicationException {
        PlatformEntity thing = newThing;
        if (!thing.isProcesssed()) {
            if (thing.getIdentifier() != null && !thing.isSetName()) {
                return getRepository().findByIdentifier(thing.getIdentifier()).get();
            }
            if (thing.getIdentifier() == null) {
                if (getRepository().existsByName(thing.getName())) {
                    Optional<PlatformEntity> optional = getRepository().findByName(thing.getName());
                    return optional.orElse(null);
                } else {
                    // Autogenerate Identifier
                    thing.setIdentifier(UUID.randomUUID().toString());
                }
            } else if (getRepository().existsByIdentifier(thing.getIdentifier())) {
                throw new ODataApplicationException("Identifier already exists!",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
            }
            thing.setProcesssed(true);
            processLocations(thing);
            thing = getRepository().intermediateSave(thing);
            processHistoricalLocations(thing);
            processDatastreams(thing);
            thing = getRepository().save(thing);
        }
        return thing;

    }

    @Override
    public PlatformEntity update(PlatformEntity entity, HttpMethod method) throws ODataApplicationException {
        checkUpdate(entity);
        if (HttpMethod.PATCH.equals(method)) {
            Optional<PlatformEntity> existing = getRepository().findByIdentifier(entity.getIdentifier());
            if (existing.isPresent()) {
                PlatformEntity merged = mapper.merge(existing.get(), entity);
                if (entity.hasLocationEntities()) {
                    merged.setLocations(entity.getLocations());
                    processLocations(merged);
                    merged = getRepository().save(merged);
                    processHistoricalLocations(merged);
                }
                return getRepository().save(merged);
            }
            throw new ODataApplicationException(
                    "Unable to update. Entity not found.",
                    HttpStatusCode.NOT_FOUND.getStatusCode(),
                    Locale.ROOT);
        } else if (HttpMethod.PUT.equals(method)) {
            throw new ODataApplicationException("Http PUT is not yet supported!",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }
        throw new ODataApplicationException("Invalid http method for updating entity!",
                HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
    }

    @Override
    public PlatformEntity update(PlatformEntity entity) {
        return getRepository().save(entity);
    }

    private void checkUpdate(PlatformEntity thing) throws ODataApplicationException {
        if (thing.hasLocationEntities()) {
            for (LocationEntity location : thing.getLocations()) {
                checkInlineLocation(location);
            }
        }
        if (thing.hasDatastreams()) {
            for (DatastreamEntity datastream : thing.getDatastreams()) {
                checkInlineDatastream(datastream);
            }
        }
    }

    @Override
    public void delete(String identifier) throws ODataApplicationException {
        if (getRepository().existsByIdentifier(identifier)) {
            PlatformEntity thing = getRepository().getOneByIdentifier(identifier);
            // delete datastreams
            thing.getDatastreams().forEach(d -> {
                try {
                    getDatastreamService().delete(d.getIdentifier());
                } catch (ODataApplicationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            // delete historicalLocation
            thing.getHistoricalLocations().forEach(hl -> {
                try {
                    getHistoricalLocationService().delete(hl);
                } catch (ODataApplicationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            getRepository().deleteByIdentifier(identifier);
        } else {
            throw new ODataApplicationException(
                    "Unable to delete. Entity not found.",
                    HttpStatusCode.NOT_FOUND.getStatusCode(),
                    Locale.ROOT);
        }
    }

    @Override
    public void delete(PlatformEntity entity) {
        getRepository().deleteByIdentifier(entity.getIdentifier());
    }

    @Override
    protected PlatformEntity createOrUpdate(PlatformEntity entity) throws ODataApplicationException {
        if (entity.getIdentifier() != null && getRepository().existsByIdentifier(entity.getIdentifier())) {
            return update(entity, HttpMethod.PATCH);
        }
        return create(entity);
    }

    private void processDatastreams(PlatformEntity thing) throws ODataApplicationException {
        if (thing.hasDatastreams()) {
            Set<DatastreamEntity> datastreams = new LinkedHashSet<>();
            for (DatastreamEntity datastream : thing.getDatastreams()) {
                datastream.setThing(thing);
                DatastreamEntity optionalDatastream = getDatastreamService().create(datastream);
                datastreams.add(optionalDatastream != null ? optionalDatastream : datastream);
            }
            thing.setDatastreams(datastreams);
        }
    }

    private void processLocations(PlatformEntity thing) throws ODataApplicationException {
        if (thing.hasLocationEntities()) {
            Set<LocationEntity> locations = new LinkedHashSet<>();
            for (LocationEntity location : thing.getLocations()) {
                LocationEntity optionalLocation = getLocationService().create(location);
                locations.add(optionalLocation != null ? optionalLocation : location);
            }
            thing.setLocations(locations);
        }
    }

    private void processHistoricalLocations(PlatformEntity thing) throws ODataApplicationException {
        if (thing != null && thing.hasLocationEntities()) {
            Set<HistoricalLocationEntity> historicalLocations = thing.hasHistoricalLocations()
                    ? new LinkedHashSet<>(thing.getHistoricalLocations())
                    : new LinkedHashSet<>();
            HistoricalLocationEntity historicalLocation = new HistoricalLocationEntity();
            historicalLocation.setIdentifier(UUID.randomUUID().toString());
            historicalLocation.setThing(thing);
            historicalLocation.setTime(DateTime.now().toDate());
            historicalLocation.setProcesssed(true);
            HistoricalLocationEntity createdHistoricalLocation =
                    getHistoricalLocationService().createOrUpdate(historicalLocation);
            if (createdHistoricalLocation != null) {
                historicalLocations.add(createdHistoricalLocation);
            }
            for (LocationEntity location : thing.getLocations()) {
                location.setHistoricalLocations(historicalLocations);
                getLocationService().createOrUpdate(location);
            }
            thing.setHistoricalLocations(historicalLocations);
        }
    }

    @SuppressWarnings("unchecked")
    private AbstractSensorThingsEntityService<?, LocationEntity> getLocationService() {
        return (AbstractSensorThingsEntityService<?, LocationEntity>) getEntityService(EntityTypes.Location);
    }

    @SuppressWarnings("unchecked")
    private AbstractSensorThingsEntityService<?, HistoricalLocationEntity> getHistoricalLocationService() {
        return (AbstractSensorThingsEntityService<?, HistoricalLocationEntity>) getEntityService(
                EntityTypes.HistoricalLocation);
    }

    @SuppressWarnings("unchecked")
    private AbstractSensorThingsEntityService<?, DatastreamEntity> getDatastreamService() {
        return (AbstractSensorThingsEntityService<?, DatastreamEntity>) getEntityService(
                EntityTypes.Datastream);
    }

    @Override
    public Map<String, Set<String>> getRelatedCollections(Object rawObject) {
        Map<String, Set<String>> collections = new HashMap<>();
        PlatformEntity entity = (PlatformEntity) rawObject;

        if (entity.hasLocationEntities()) {
            collections.put(
                    LocationEntityProvider.ET_LOCATION_NAME,
                    entity.getLocations()
                            .stream()
                            .map(LocationEntity::getIdentifier)
                            .collect(Collectors.toSet()));
        }

        if (entity.hasHistoricalLocations()) {
            collections.put(
                    HistoricalLocationEntityProvider.ET_HISTORICAL_LOCATION_NAME,
                    entity.getHistoricalLocations()
                            .stream()
                            .map(HistoricalLocationEntity::getIdentifier)
                            .collect(Collectors.toSet()));
        }

        if (entity.hasDatastreams()) {
            collections.put(DatastreamEntityProvider.ET_DATASTREAM_NAME,
                    entity.getDatastreams()
                            .stream()
                            .map(DatastreamEntity::getIdentifier)
                            .collect(Collectors.toSet()));
        }
        return collections;
    }

}
