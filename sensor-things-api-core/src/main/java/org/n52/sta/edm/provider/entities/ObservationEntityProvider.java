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
import org.n52.sta.edm.provider.complextypes.OpenComplexType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:j.speckamp@52north.org">Jan Speckamp</a>
 */
@Component
public class ObservationEntityProvider extends AbstractSensorThingsEntityProvider {

    // Entity Type Name
    public static final String ET_OBSERVATION_NAME = "Observation";
    public static final FullQualifiedName ET_OBSERVATION_FQN =
            new FullQualifiedName(SensorThingsEdmConstants.NAMESPACE, ET_OBSERVATION_NAME);

    // Entity Set Name
    public static final String ES_OBSERVATIONS_NAME = "Observations";

    @Override
    protected CsdlEntityType createEntityType() {

        List<CsdlProperty> properties = createCsdlProperties();

        List<CsdlNavigationProperty> navigationProperties = createCsdlNavigationProperties();

        // create CsdlPropertyRef for Key element
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName(PROP_ID);

        // configure EntityType
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(ET_OBSERVATION_NAME);
        entityType.setProperties(properties);
        entityType.setKey(Collections.singletonList(propertyRef));
        entityType.setNavigationProperties(navigationProperties);

        return entityType;

    }

    @Override
    protected CsdlEntitySet createEntitySet() {
        CsdlEntitySet entitySet = new CsdlEntitySet();
        entitySet.setName(ES_OBSERVATIONS_NAME);
        entitySet.setType(ET_OBSERVATION_FQN);

        CsdlNavigationPropertyBinding navPropDatastreamBinding = new CsdlNavigationPropertyBinding();
        navPropDatastreamBinding.setPath(DatastreamEntityProvider.ET_DATASTREAM_NAME);
        navPropDatastreamBinding.setTarget(DatastreamEntityProvider.ES_DATASTREAMS_NAME);

        CsdlNavigationPropertyBinding navPropFeatureOfInterestBinding = new CsdlNavigationPropertyBinding();
        navPropFeatureOfInterestBinding.setPath(FeatureOfInterestEntityProvider.ET_FEATURE_OF_INTEREST_NAME);
        navPropFeatureOfInterestBinding.setTarget(FeatureOfInterestEntityProvider.ES_FEATURES_OF_INTEREST_NAME);

        List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
        navPropBindingList.add(navPropDatastreamBinding);
        navPropBindingList.add(navPropFeatureOfInterestBinding);
        entitySet.setNavigationPropertyBindings(navPropBindingList);

        return entitySet;
    }

    @Override
    public FullQualifiedName getFullQualifiedTypeName() {
        return ET_OBSERVATION_FQN;
    }

    private List<CsdlProperty> createCsdlProperties() {
        //create EntityType primitive properties
        CsdlProperty id = new CsdlProperty().setName(PROP_ID)
                .setType(EdmPrimitiveTypeKind.Any.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty phenomenonTime = new CsdlProperty().setName(PROP_PHENOMENON_TIME)
                .setType(EdmPrimitiveTypeKind.Timespan.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty result = new CsdlProperty().setName(PROP_RESULT)
                .setType(EdmPrimitiveTypeKind.Any.getFullQualifiedName())
                .setNullable(false);
        CsdlProperty resultTime = new CsdlProperty().setName(PROP_RESULT_TIME)
                .setType(EdmPrimitiveTypeKind.DateTimeOffset.getFullQualifiedName())
                .setNullable(true)
                .setPrecision(9);
        CsdlProperty resultQuality = new CsdlProperty().setName(PROP_RESULT_QUALITY)
                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName())
                .setNullable(true);
        CsdlProperty validTime = new CsdlProperty().setName(PROP_VALID_TIME)
                .setType(EdmPrimitiveTypeKind.Timespan.getFullQualifiedName())
                .setNullable(true);
        CsdlProperty parameters = new CsdlProperty().setName(PROP_PARAMETERS)
                .setType(OpenComplexType.CT_OPEN_TYPE_FQN)
                .setCollection(true)
                .setNullable(true);

        return Arrays.asList(
                id,
                phenomenonTime,
                result,
                resultTime,
                resultQuality,
                validTime,
                parameters);
    }

    private List<CsdlNavigationProperty> createCsdlNavigationProperties() {
        // navigation property: Many optional to one mandatory
        CsdlNavigationProperty navPropDatastreams = new CsdlNavigationProperty()
                .setName(DatastreamEntityProvider.ET_DATASTREAM_NAME)
                .setType(DatastreamEntityProvider.ET_DATASTREAM_FQN)
                .setNullable(false)
                .setPartner(ES_OBSERVATIONS_NAME);

        // navigation property: Many optional to one mandatory
        CsdlNavigationProperty navPropFeatureOfInterest = new CsdlNavigationProperty()
                .setName(FeatureOfInterestEntityProvider.ET_FEATURE_OF_INTEREST_NAME)
                .setType(FeatureOfInterestEntityProvider.ET_FEATURE_OF_INTEREST_FQN)
                .setNullable(false)
                .setPartner(ES_OBSERVATIONS_NAME);

        List<CsdlNavigationProperty> navPropList = new ArrayList<CsdlNavigationProperty>();

        return Arrays.asList(
                navPropDatastreams,
                navPropFeatureOfInterest);
    }
}
