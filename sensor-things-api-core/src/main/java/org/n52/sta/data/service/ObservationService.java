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
import org.n52.series.db.beans.AbstractFeatureEntity;
import org.n52.series.db.beans.BooleanDataEntity;
import org.n52.series.db.beans.CategoryDataEntity;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.CountDataEntity;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.FormatEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.QuantityDataEntity;
import org.n52.series.db.beans.TextDataEntity;
import org.n52.series.db.beans.dataset.DatasetType;
import org.n52.series.db.beans.dataset.ObservationType;
import org.n52.series.db.beans.dataset.ValueType;
import org.n52.series.db.beans.sta.DatastreamEntity;
import org.n52.series.db.beans.sta.LocationEntity;
import org.n52.series.db.beans.sta.StaDataEntity;
import org.n52.shetland.ogc.om.OmConstants;
import org.n52.sta.data.query.DatasetQuerySpecifications;
import org.n52.sta.data.query.DatastreamQuerySpecifications;
import org.n52.sta.data.query.ObservationQuerySpecifications;
import org.n52.sta.data.repositories.CategoryRepository;
import org.n52.sta.data.repositories.DataRepository;
import org.n52.sta.data.repositories.DatasetRepository;
import org.n52.sta.data.repositories.DatastreamRepository;
import org.n52.sta.data.repositories.OfferingRepository;
import org.n52.sta.data.service.EntityServiceRepository.EntityTypes;
import org.n52.sta.edm.provider.entities.DatastreamEntityProvider;
import org.n52.sta.edm.provider.entities.FeatureOfInterestEntityProvider;
import org.n52.sta.mapping.FeatureOfInterestMapper;
import org.n52.sta.mapping.ObservationMapper;
import org.n52.sta.service.query.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * @author <a href="mailto:j.speckamp@52north.org">Jan Speckamp</a>
 */
@Component
@DependsOn({"springApplicationContext"})
public class ObservationService extends
        AbstractSensorThingsEntityService<DataRepository<DataEntity<?>>, DataEntity<?>> {

    private static final Logger logger = LoggerFactory.getLogger(ObservationService.class);

    private static final ObservationQuerySpecifications oQS = new ObservationQuerySpecifications();
    private static final DatasetQuerySpecifications dQS = new DatasetQuerySpecifications();
    private static final DatastreamQuerySpecifications dsQS = new DatastreamQuerySpecifications();

    private final CategoryRepository categoryRepository;
    private final OfferingRepository offeringRepository;
    private final DatastreamRepository datastreamRepository;
    private final DatasetRepository datasetRepository;
    private final String IOT_DATASTREAM = "iot.Datastream";
    private final String IOT_FEATUREOFINTEREST = "iot.FeatureOfInterest";

    private final ObservationMapper mapper;
    private final FeatureOfInterestMapper featureMapper;


    @Autowired
    public ObservationService(DataRepository<DataEntity<?>> repository,
                              ObservationMapper mapper,
                              FeatureOfInterestMapper featureMapper,
                              CategoryRepository categoryRepository,
                              OfferingRepository offeringRepository,
                              DatastreamRepository datastreamRepository,
                              DatasetRepository datasetRepository) {
        super(repository);
        this.mapper = mapper;
        this.featureMapper = featureMapper;
        this.categoryRepository = categoryRepository;
        this.offeringRepository = offeringRepository;
        this.datastreamRepository = datastreamRepository;
        this.datasetRepository = datasetRepository;
    }

    @Override
    public EntityTypes getType() {
        return EntityTypes.Observation;
    }

    @Override
    public EntityCollection getEntityCollection(QueryOptions queryOptions) throws ODataApplicationException {
        EntityCollection retEntitySet = new EntityCollection();
        Specification<DataEntity<?>> filter = getFilterPredicate(DataEntity.class, queryOptions);
        getRepository().findAll(filter, createPageableRequest(queryOptions))
                .forEach(t -> retEntitySet.getEntities().add(mapper.createEntity(t)));
        return retEntitySet;
    }

    @Override
    public Entity getEntity(String id) {
        Optional<DataEntity<?>> entity = getRepository().findByIdentifier(id);
        return entity.isPresent() ? mapper.createEntity(entity.get()) : null;
    }

    @Override
    public EntityCollection getRelatedEntityCollection(String sourceId,
                                                       EdmEntityType sourceEntityType,
                                                       QueryOptions queryOptions)
            throws ODataApplicationException {
        Specification<DataEntity<?>> filter = getFilter(sourceId, sourceEntityType);
        filter = filter.and(getFilterPredicate(DataEntity.class, queryOptions));
        // TODO: check cast
        Iterable<DataEntity<?>> observations = getRepository().findAll(filter, createPageableRequest(queryOptions));
        EntityCollection retEntitySet = new EntityCollection();
        observations.forEach(t -> retEntitySet.getEntities().add(mapper.createEntity(t)));
        return retEntitySet;
    }

    @Override
    public long getRelatedEntityCollectionCount(String sourceId, EdmEntityType sourceEntityType) {
        Specification<DataEntity<?>> filter = getFilter(sourceId, sourceEntityType);
        return getRepository().count(filter);
    }

    private Specification<DataEntity<?>> getFilter(String sourceIdentifier, EdmEntityType sourceEntityType) {
        Specification<DataEntity<?>> filter;
        switch (sourceEntityType.getFullQualifiedName().getFullQualifiedNameAsString()) {
            case IOT_DATASTREAM: {
                filter = oQS.withDatastreamIdentifier(sourceIdentifier);
                break;
            }
            case IOT_FEATUREOFINTEREST: {
                filter = oQS.withFeatureOfInterestIdentifier(sourceIdentifier);
                break;
            }
            default:
                return null;
        }
        return filter;
    }

    @Override
    public boolean existsEntity(String id) {
        return getRepository().existsByIdentifier(id);
    }

    @Override
    public boolean existsRelatedEntity(String sourceId, EdmEntityType sourceEntityType) {
        return this.existsRelatedEntity(sourceId, sourceEntityType, null);
    }

    @Override
    public boolean existsRelatedEntity(String sourceId, EdmEntityType sourceEntityType, String targetId) {
        Specification<DataEntity<?>> filter;
        switch (sourceEntityType.getFullQualifiedName().getFullQualifiedNameAsString()) {
            case IOT_DATASTREAM: {
                filter = oQS.withDatastreamIdentifier(sourceId);
                break;
            }
            case IOT_FEATUREOFINTEREST: {
                filter = oQS.withFeatureOfInterestIdentifier(sourceId);
                break;
            }
            default:
                return false;
        }
        if (targetId != null) {
            filter = filter.and(oQS.withIdentifier(targetId));
        }
        return getRepository().count(filter) > 0;
    }

    @Override
    public Optional<String> getIdForRelatedEntity(String sourceId, EdmEntityType sourceEntityType) {
        return this.getIdForRelatedEntity(sourceId, sourceEntityType, null);
    }

    @Override
    public Optional<String> getIdForRelatedEntity(String sourceId, EdmEntityType sourceEntityType, String targetId) {
        Optional<DataEntity<?>> observation = this.getRelatedEntityRaw(sourceId, sourceEntityType, targetId);
        return observation.map(dataEntity -> Optional.of(dataEntity.getIdentifier())).orElseGet(Optional::empty);
    }

    @Override
    public Entity getRelatedEntity(String sourceId, EdmEntityType sourceEntityType) {
        return this.getRelatedEntity(sourceId, sourceEntityType, null);
    }

    @Override
    public Entity getRelatedEntity(String sourceId, EdmEntityType sourceEntityType, String targetId) {
        Optional<DataEntity<?>> observation = this.getRelatedEntityRaw(sourceId, sourceEntityType, targetId);
        return observation.map(dataEntity -> mapper.createEntity(dataEntity)).orElse(null);
    }

    @Override
    public String checkPropertyName(String property) {
        switch (property) {
            case "phenomenonTime":
                // TODO: proper ISO8601 comparison
                return DataEntity.PROPERTY_SAMPLING_TIME_END;
            case "result":
                return DataEntity.PROPERTY_VALUE;
            default:
                return super.checkPropertyName(property);
        }
    }

    /**
     * Retrieves Observation Entity with Relation to sourceEntity from Database. Returns empty if Observation
     * is not found or Entities are not related.
     *
     * @param sourceId         Id of the Source Entity
     * @param sourceEntityType Type of the Source Entity
     * @param targetId         Id of the Thing to be retrieved
     * @return Optional&lt;DataEntity &lt; ?&gt;&gt; Requested Entity
     */
    private Optional<DataEntity<?>> getRelatedEntityRaw(String sourceId,
                                                        EdmEntityType sourceEntityType,
                                                        String targetId) {
        Specification<DataEntity<?>> filter;
        switch (sourceEntityType.getFullQualifiedName().getFullQualifiedNameAsString()) {
            case IOT_DATASTREAM: {
                filter = oQS.withDatastreamIdentifier(sourceId);
                break;
            }
            case IOT_FEATUREOFINTEREST: {
                filter = oQS.withFeatureOfInterestIdentifier(sourceId);
                break;
            }
            default:
                return Optional.empty();
        }

        if (targetId != null) {
            filter = filter.and(oQS.withIdentifier(targetId));
        }
        return getRepository().findOne(filter);
    }

    @Override
    public long getCount(QueryOptions queryOptions) throws ODataApplicationException {
        return getRepository().count(getFilterPredicate(DataEntity.class, queryOptions));
    }

    @Override
    public DataEntity<?> create(DataEntity<?> entity) throws ODataApplicationException {
        if (entity instanceof StaDataEntity) {
            StaDataEntity observation = (StaDataEntity) entity;
            if (!observation.isProcesssed()) {
                observation.setProcesssed(true);
                check(observation);
                DatastreamEntity datastream = checkDatastream(observation);

                AbstractFeatureEntity<?> feature = checkFeature(observation, datastream);
                // category (obdProp)
                CategoryEntity category = checkCategory(datastream);
                // offering (sensor)
                OfferingEntity offering = checkOffering(datastream);
                // dataset
                DatasetEntity dataset = checkDataset(datastream, feature, category, offering);
                // observation
                DataEntity<?> data = checkData(observation, dataset);
                if (data != null) {
                    updateDataset(dataset, data);
                    updateDatastream(datastream, dataset);
                }
                return data;
            }
            return observation;
        }
        return entity;
    }

    private void check(StaDataEntity observation) throws ODataApplicationException {
        if (observation.getDatastream() == null) {
            throw new ODataApplicationException("The observation to create is invalid. Missing datastream!",
                    HttpStatusCode.BAD_REQUEST.getStatusCode(),
                    Locale.getDefault());
        }
    }

    @Override
    public DataEntity<?> update(DataEntity<?> entity, HttpMethod method) throws ODataApplicationException {
        if (HttpMethod.PATCH.equals(method)) {
            Optional<DataEntity<?>> existing = getRepository().findByIdentifier(entity.getIdentifier());
            if (existing.isPresent()) {
                DataEntity<?> merged = mapper.merge(existing.get(), entity);
                return getRepository().save(merged);
            }
            throw new ODataApplicationException(
                    "Unable to update. Entity not found.",
                    HttpStatusCode.NOT_FOUND.getStatusCode(),
                    Locale.ROOT);
        } else if (HttpMethod.PUT.equals(method)) {
            throw new ODataApplicationException("Http PUT is not yet supported!",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
                    Locale.getDefault());
        }
        throw new ODataApplicationException("Invalid http method for updating entity!",
                HttpStatusCode.BAD_REQUEST.getStatusCode(),
                Locale.getDefault());
    }

    @Override
    public DataEntity<?> update(DataEntity<?> entity) {
        return getRepository().save(entity);
    }

    @Override
    public void delete(String identifier) throws ODataApplicationException {
        if (getRepository().existsByIdentifier(identifier)) {
            DataEntity<?> observation = getRepository().getOneByIdentifier(identifier);
            checkDataset(observation);
            delete(observation);
        } else {
            throw new ODataApplicationException(
                    "Unable to delete. Entity not found.",
                    HttpStatusCode.NOT_FOUND.getStatusCode(),
                    Locale.ROOT);
        }
    }

    @Override
    public void delete(DataEntity<?> entity) {
        getRepository().deleteByIdentifier(entity.getIdentifier());
    }

    @Override
    protected DataEntity<?> createOrUpdate(DataEntity<?> entity) throws ODataApplicationException {
        if (entity.getIdentifier() != null && getRepository().existsByIdentifier(entity.getIdentifier())) {
            return update(entity, HttpMethod.PATCH);
        }
        return create(entity);
    }

    private void checkDataset(DataEntity<?> observation) {
        // TODO get the next first/last observation and set it
        DatasetEntity dataset = observation.getDataset();
        if (dataset.getFirstObservation() != null
                && dataset.getFirstObservation().getIdentifier().equals(observation.getIdentifier())) {
            dataset.setFirstObservation(null);
            dataset.setFirstQuantityValue(null);
            dataset.setFirstValueAt(null);
        }
        if (dataset.getLastObservation() != null && dataset.getLastObservation()
                                                           .getIdentifier()
                                                           .equals(observation.getIdentifier())) {
            dataset.setLastObservation(null);
            dataset.setLastQuantityValue(null);
            dataset.setLastValueAt(null);
        }
        observation.setDataset(datasetRepository.saveAndFlush(dataset));
    }

    private DatasetEntity checkDataset(DatastreamEntity datastream,
                                       AbstractFeatureEntity<?> feature,
                                       CategoryEntity category,
                                       OfferingEntity offering) {
        DatasetEntity dataset = getDatasetEntity(datastream.getObservationType().getFormat());
        dataset.setProcedure(datastream.getProcedure());
        dataset.setPhenomenon(datastream.getObservableProperty());
        dataset.setCategory(category);
        dataset.setFeature(feature);
        dataset.setOffering(offering);
        dataset.setPlatform(dataset.getPlatform());
        dataset.setUnit(datastream.getUnit());
        dataset.setOmObservationType(datastream.getObservationType());
        Specification<DatasetEntity> query = dQS.matchProcedures(datastream.getProcedure().getIdentifier())
                .and(dQS.matchPhenomena(datastream.getObservableProperty().getIdentifier())
                        .and(dQS.matchFeatures(feature.getIdentifier()))
                        .and(dQS.matchOfferings(offering.getIdentifier())));
        Optional<DatasetEntity> queried = datasetRepository.findOne(query);
        if (queried.isPresent()) {
            return queried.get();
        } else {
            return datasetRepository.save(dataset);
        }
    }

    DatastreamEntity checkDatastream(StaDataEntity observation) throws ODataApplicationException {
        DatastreamEntity datastream = getDatastreamService().create(observation.getDatastream());
        observation.setDatastream(datastream);
        return datastream;
    }

    private AbstractFeatureEntity<?> checkFeature(StaDataEntity observation, DatastreamEntity datastream)
            throws ODataApplicationException {
        if (!observation.hasFeatureOfInterest()) {
            AbstractFeatureEntity<?> feature = null;
            for (LocationEntity location : datastream.getThing().getLocations()) {
                if (feature == null) {
                    feature = featureMapper.createFeatureOfInterest(location);
                }
                if (location.isSetGeometry()) {
                    feature = featureMapper.createFeatureOfInterest(location);
                    break;
                }
            }
            if (feature == null) {
                throw new ODataApplicationException("The observation to create is invalid." +
                        " Missing feature or thing.location!",
                        HttpStatusCode.BAD_REQUEST.getStatusCode(),
                        Locale.getDefault());
            }
            observation.setFeatureOfInterest(feature);
        }
        AbstractFeatureEntity<?> feature = getFeatureOfInterestService().create(observation.getFeatureOfInterest());
        observation.setFeatureOfInterest(feature);
        return feature;
    }

    private OfferingEntity checkOffering(DatastreamEntity datastream) {
        OfferingEntity offering = new OfferingEntity();
        ProcedureEntity procedure = datastream.getProcedure();
        offering.setIdentifier(procedure.getIdentifier());
        offering.setName(procedure.getName());
        offering.setDescription(procedure.getDescription());
        if (datastream.hasSamplingTimeStart()) {
            offering.setSamplingTimeStart(datastream.getSamplingTimeStart());
        }
        if (datastream.hasSamplingTimeEnd()) {
            offering.setSamplingTimeEnd(datastream.getSamplingTimeEnd());
        }
        if (datastream.getResultTimeStart() != null) {
            offering.setResultTimeStart(datastream.getResultTimeStart());
        }
        if (datastream.getResultTimeEnd() != null) {
            offering.setResultTimeEnd(datastream.getResultTimeEnd());
        }
        if (datastream.isSetGeometry()) {
            offering.setGeometryEntity(datastream.getGeometryEntity());
        }
        HashSet<FormatEntity> set = new HashSet<>();
        set.add(datastream.getObservationType());
        offering.setObservationTypes(set);

        if (!offeringRepository.existsByIdentifier(offering.getIdentifier())) {
            return offeringRepository.save(offering);
        } else {
            // TODO expand time and geometry if necessary
            return offeringRepository.findByIdentifier(offering.getIdentifier()).get();
        }
    }

    private CategoryEntity checkCategory(DatastreamEntity datastream) {
        CategoryEntity category = new CategoryEntity();
        PhenomenonEntity obsProp = datastream.getObservableProperty();
        category.setIdentifier(obsProp.getStaIdentifier());
        category.setName(obsProp.getName());
        category.setDescription(obsProp.getDescription());
        if (!categoryRepository.existsByIdentifier(category.getIdentifier())) {
            return categoryRepository.save(category);
        } else {
            return categoryRepository.findByIdentifier(category.getIdentifier()).get();
        }
    }

    private DataEntity<?> checkData(StaDataEntity observation, DatasetEntity dataset) throws ODataApplicationException {
        DataEntity<?> data = getDataEntity(observation, dataset);
        if (data != null) {
            return getRepository().save(data);
        }
        return null;
    }

    private DatasetEntity updateDataset(DatasetEntity dataset, DataEntity<?> data) {
        if (!dataset.isSetFirstValueAt()
                || (dataset.isSetFirstValueAt() && data.getSamplingTimeStart().before(dataset.getFirstValueAt()))) {
            dataset.setFirstValueAt(data.getSamplingTimeStart());
            dataset.setFirstObservation(data);
            if (data instanceof QuantityDataEntity) {
                dataset.setFirstQuantityValue(((QuantityDataEntity) data).getValue());
            }
        }
        if (!dataset.isSetLastValueAt()
                || (dataset.isSetLastValueAt() && data.getSamplingTimeEnd().after(dataset.getLastValueAt()))) {
            dataset.setLastValueAt(data.getSamplingTimeEnd());
            dataset.setLastObservation(data);
            if (data instanceof QuantityDataEntity) {
                dataset.setLastQuantityValue(((QuantityDataEntity) data).getValue());
            }
        }
        return datasetRepository.save(dataset);
    }

    private void updateDatastream(DatastreamEntity datastream, DatasetEntity dataset) throws ODataApplicationException {
        if (datastream.getDatasets() != null && !datastream.getDatasets().contains(dataset)) {
            datastream.addDataset(dataset);
            getDatastreamService().update(datastream);
        }
    }

    @SuppressWarnings("unchecked")
    AbstractSensorThingsEntityService<?, DatastreamEntity> getDatastreamService() {
        return (AbstractSensorThingsEntityService<?, DatastreamEntity>) getEntityService(EntityTypes.Datastream);
    }

    @SuppressWarnings("unchecked")
    private AbstractSensorThingsEntityService<?, AbstractFeatureEntity<?>> getFeatureOfInterestService() {
        return (AbstractSensorThingsEntityService<?, AbstractFeatureEntity<?>>)
                getEntityService(EntityTypes.FeatureOfInterest);
    }

    private DatasetEntity getDatasetEntity(String observationType) {
        DatasetEntity dataset = new DatasetEntity().setObservationType(ObservationType.simple)
                .setDatasetType(DatasetType.timeseries);
        switch (observationType) {
            case OmConstants.OBS_TYPE_MEASUREMENT:
                return dataset.setValueType(ValueType.quantity);
            case OmConstants.OBS_TYPE_CATEGORY_OBSERVATION:
                return dataset.setValueType(ValueType.category);
            case OmConstants.OBS_TYPE_COUNT_OBSERVATION:
                return dataset.setValueType(ValueType.count);
            case OmConstants.OBS_TYPE_TEXT_OBSERVATION:
                return dataset.setValueType(ValueType.text);
            case OmConstants.OBS_TYPE_TRUTH_OBSERVATION:
                return dataset.setValueType(ValueType.bool);
            default:
                return dataset;
        }
    }

    private DataEntity<?> getDataEntity(StaDataEntity observation, DatasetEntity dataset)
            throws ODataApplicationException {
        DataEntity<?> data = null;
        switch (dataset.getOmObservationType().getFormat()) {
            case OmConstants.OBS_TYPE_MEASUREMENT:
                QuantityDataEntity quantityDataEntity = new QuantityDataEntity();
                if (observation.hasValue()) {
                    String obs = observation.getValue();
                    if (obs.equals("NaN") || obs.equals("Inf") || obs.equals("-Inf")) {
                        quantityDataEntity.setValue(null);
                    } else {
                        quantityDataEntity.setValue(BigDecimal.valueOf(Double.parseDouble(observation.getValue())));
                    }
                }
                data = quantityDataEntity;
                break;
            case OmConstants.OBS_TYPE_CATEGORY_OBSERVATION:
                CategoryDataEntity categoryDataEntity = new CategoryDataEntity();
                if (observation.hasValue()) {
                    categoryDataEntity.setValue(observation.getValue());
                }
                data = categoryDataEntity;
                break;
            case OmConstants.OBS_TYPE_COUNT_OBSERVATION:
                CountDataEntity countDataEntity = new CountDataEntity();
                if (observation.hasValue()) {
                    countDataEntity.setValue(Integer.parseInt(observation.getValue()));
                }
                data = countDataEntity;
                break;
            case OmConstants.OBS_TYPE_TEXT_OBSERVATION:
                TextDataEntity textDataEntity = new TextDataEntity();
                if (observation.hasValue()) {
                    textDataEntity.setValue(observation.getValue());
                }
                data = textDataEntity;
                break;
            case OmConstants.OBS_TYPE_TRUTH_OBSERVATION:
                BooleanDataEntity booleanDataEntity = new BooleanDataEntity();
                if (observation.hasValue()) {
                    booleanDataEntity.setValue(Boolean.parseBoolean(observation.getValue()));
                }
                data = booleanDataEntity;
                break;
            default:
                break;
        }
        if (data != null) {
            data.setDataset(dataset);
            if (observation.getIdentifier() != null) {
                if (getRepository().existsByIdentifier(observation.getIdentifier())) {
                    throw new ODataApplicationException("Identifier already exists!",
                            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.getDefault());
                } else {
                    data.setIdentifier(observation.getIdentifier());
                }
            } else {
                data.setIdentifier(UUID.randomUUID().toString());
            }
            data.setSamplingTimeStart(observation.getSamplingTimeStart());
            data.setSamplingTimeEnd(observation.getSamplingTimeEnd());
            if (observation.getResultTime() != null) {
                data.setResultTime(observation.getResultTime());
            } else {
                data.setResultTime(observation.getSamplingTimeEnd());
            }
            data.setValidTimeStart(observation.getValidTimeStart());
            data.setValidTimeEnd(observation.getValidTimeEnd());
        }
        return data;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.n52.sta.mapping.AbstractMapper#getRelatedCollections(java.lang.Object)
     */
    @Override
    public Map<String, Set<String>> getRelatedCollections(Object rawObject) {
        Map<String, Set<String>> collections = new HashMap<>();
        DataEntity<?> entity = (DataEntity<?>) rawObject;

        if (entity.getDataset() != null && entity.getDataset().getFeature() != null) {
            collections.put(FeatureOfInterestEntityProvider.ET_FEATURE_OF_INTEREST_NAME,
                    Collections.singleton(entity.getDataset().getFeature().getIdentifier()));
        }

        Optional<DatastreamEntity> datastreamEntity =
                datastreamRepository.findOne(dsQS.withObservationIdentifier(entity.getIdentifier()));
        if (datastreamEntity.isPresent()) {
            collections.put(DatastreamEntityProvider.ET_DATASTREAM_NAME,
                    Collections.singleton(datastreamEntity.get().getIdentifier()));
        } else {
            logger.debug("No Datastream associated with this Entity {}", entity.getIdentifier());
        }
        return collections;
    }

}
