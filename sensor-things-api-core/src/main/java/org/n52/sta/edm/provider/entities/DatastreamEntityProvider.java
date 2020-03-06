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
package org.n52.sta.edm.provider.entities;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.n52.sta.edm.provider.SensorThingsEdmConstants;
import org.n52.sta.edm.provider.complextypes.UnitOfMeasurementComplexType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:s.drost@52north.org">Sebastian Drost</a>
 * @author <a href="mailto:j.speckamp@52north.org">Jan Speckamp</a>
 */
@Component
public class DatastreamEntityProvider extends AbstractSensorThingsEntityProvider {

    // Entity Type Name
    public static final String ET_DATASTREAM_NAME = "Datastream";
    public static final FullQualifiedName ET_DATASTREAM_FQN =
            new FullQualifiedName(SensorThingsEdmConstants.NAMESPACE, ET_DATASTREAM_NAME);

    // Entity Set Name
    public static final String ES_DATASTREAMS_NAME = "Datastreams";

    @Override
    protected CsdlEntityType createEntityType() {
        List<CsdlProperty> properties = createCsdlProperties();

        List<CsdlNavigationProperty> navigationProperties = createCsdlNavigationProperties();

        // create CsdlPropertyRef for Key element
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName(PROP_ID);

        // configure EntityType
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(ET_DATASTREAM_NAME);
        entityType.setProperties(properties);
        entityType.setKey(Collections.singletonList(propertyRef));
        entityType.setNavigationProperties(navigationProperties);

        return entityType;
    }

    @Override
    protected CsdlEntitySet createEntitySet() {
        CsdlEntitySet entitySet = new CsdlEntitySet();
        entitySet.setName(ES_DATASTREAMS_NAME);
        entitySet.setType(ET_DATASTREAM_FQN);

        CsdlNavigationPropertyBinding navPropThingBinding = new CsdlNavigationPropertyBinding();
        // the path from entity type to navigation property
        navPropThingBinding.setPath(ThingEntityProvider.ET_THING_NAME);
        // target entitySet, where the nav prop points to
        navPropThingBinding.setTarget(ThingEntityProvider.ES_THINGS_NAME);

        CsdlNavigationPropertyBinding navPropSensorBinding = new CsdlNavigationPropertyBinding();
        navPropSensorBinding.setPath(SensorEntityProvider.ET_SENSOR_NAME);
        navPropSensorBinding.setTarget(SensorEntityProvider.ES_SENSORS_NAME);

        CsdlNavigationPropertyBinding navPropObservedPropertyBinding = new CsdlNavigationPropertyBinding();
        navPropObservedPropertyBinding.setPath(ObservedPropertyEntityProvider.ET_OBSERVED_PROPERTY_NAME);
        navPropObservedPropertyBinding.setTarget(ObservedPropertyEntityProvider.ES_OBSERVED_PROPERTIES_NAME);

        CsdlNavigationPropertyBinding navPropObservationBinding = new CsdlNavigationPropertyBinding();
        navPropObservationBinding.setPath(ObservationEntityProvider.ES_OBSERVATIONS_NAME);
        navPropObservationBinding.setTarget(ObservationEntityProvider.ES_OBSERVATIONS_NAME);

        List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
        navPropBindingList.addAll(Arrays.asList(
                navPropThingBinding,
                navPropSensorBinding,
                navPropObservedPropertyBinding,
                navPropObservationBinding)
        );

        entitySet.setNavigationPropertyBindings(navPropBindingList);
        return entitySet;
    }

    @Override
    public FullQualifiedName getFullQualifiedTypeName() {
        return ET_DATASTREAM_FQN;
    }

    private List<CsdlProperty> createCsdlProperties() {
        //create EntityType properties
        CsdlProperty id = new CsdlProperty().setName(PROP_ID)
                .setType(EdmPrimitiveTypeKind.Any.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty name = new CsdlProperty().setName(PROP_NAME)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty description = new CsdlProperty().setName(PROP_DESCRIPTION)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty observationType = new CsdlProperty().setName(PROP_OBSERVATION_TYPE)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty phenomenonTime = new CsdlProperty().setName(PROP_PHENOMENON_TIME)
                .setType(EdmPrimitiveTypeKind.Timespan.getFullQualifiedName())
                .setNullable(true);
        CsdlProperty resultTime = new CsdlProperty().setName(PROP_RESULT_TIME)
                .setType(EdmPrimitiveTypeKind.Timespan.getFullQualifiedName())
                .setNullable(true);

        //create EntityType complex properties
        CsdlProperty unitOfMeasurement = new CsdlProperty().setName(PROP_UOM)
                .setType(UnitOfMeasurementComplexType.CT_UOM_FQN)
                .setNullable(false);
        CsdlProperty observedArea = new CsdlProperty().setName(PROP_OBSERVED_AREA)
                .setType(EdmPrimitiveTypeKind.Geometry.getFullQualifiedName())
                .setNullable(true);

        //create EntityType navigation links
        //CsdlProperty selfLink = new CsdlProperty().setName(SELF_LINK_ANNOTATION)
        //        .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
        //        .setNullable(false);

        return Arrays.asList(
                id,
                name,
                description,
                unitOfMeasurement,
                observationType,
                observedArea,
                phenomenonTime,
                resultTime);
    }

    private List<CsdlNavigationProperty> createCsdlNavigationProperties() {
        // navigation property: Many optional to one mandatory
        CsdlNavigationProperty navPropThings = new CsdlNavigationProperty()
                .setName(ThingEntityProvider.ET_THING_NAME)
                .setType(ThingEntityProvider.ET_THING_FQN)
                .setNullable(false)
                .setPartner(ES_DATASTREAMS_NAME);

        // navigation property: Many optional to one mandatory
        CsdlNavigationProperty navPropSensor = new CsdlNavigationProperty()
                .setName(SensorEntityProvider.ET_SENSOR_NAME)
                .setType(SensorEntityProvider.ET_SENSOR_FQN)
                .setNullable(false)
                .setPartner(ES_DATASTREAMS_NAME);

        // navigation property: Many optional to one mandatory
        CsdlNavigationProperty navPropObservedProperty = new CsdlNavigationProperty()
                .setName(ObservedPropertyEntityProvider.ET_OBSERVED_PROPERTY_NAME)
                .setType(ObservedPropertyEntityProvider.ET_OBSERVED_PROPERTY_FQN)
                .setNullable(false)
                .setPartner(ES_DATASTREAMS_NAME);

        // navigation property: One mandatory to many optional
        CsdlNavigationProperty navPropObservation = new CsdlNavigationProperty()
                .setName(ObservationEntityProvider.ES_OBSERVATIONS_NAME)
                .setType(ObservationEntityProvider.ET_OBSERVATION_FQN)
                .setCollection(true)
                .setPartner(ET_DATASTREAM_NAME);

        return Arrays.asList(
                navPropThings,
                navPropSensor,
                navPropObservedProperty,
                navPropObservation);
    }

}
