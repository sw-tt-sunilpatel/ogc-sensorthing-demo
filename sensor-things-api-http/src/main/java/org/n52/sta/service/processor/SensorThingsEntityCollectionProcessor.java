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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.n52.sta.service.processor;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.uri.UriHelperImpl;
import org.n52.sta.service.handler.AbstractEntityCollectionRequestHandler;
import org.n52.sta.service.query.QueryOptions;
import org.n52.sta.service.query.QueryOptionsHandler;
import org.n52.sta.service.query.URIQueryOptions;
import org.n52.sta.service.request.SensorThingsRequest;
import org.n52.sta.service.response.EntityCollectionResponse;
import org.n52.sta.service.serializer.SensorThingsSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;

/**
 * @author <a href="mailto:s.drost@52north.org">Sebastian Drost</a>
 */
@Component
public class SensorThingsEntityCollectionProcessor implements EntityCollectionProcessor {

    private final AbstractEntityCollectionRequestHandler<SensorThingsRequest, EntityCollectionResponse> requestHandler;
    private final QueryOptionsHandler queryOptionsHandler;
    private final String rootUrl;
    private final ODataSerializer serializer;

    private ServiceMetadata serviceMetadata;

    public SensorThingsEntityCollectionProcessor(
            AbstractEntityCollectionRequestHandler<SensorThingsRequest, EntityCollectionResponse> requestHandler,
            QueryOptionsHandler queryOptionsHandler,
            @Value("${server.rootUrl}") String rootUrl) {
        this.requestHandler = requestHandler;
        this.queryOptionsHandler = queryOptionsHandler;
        this.rootUrl = rootUrl;
        this.serializer = new SensorThingsSerializer(ContentType.JSON_NO_METADATA);
        this.queryOptionsHandler.setUriHelper(new UriHelperImpl());
    }

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
                                     ContentType contentType) throws ODataApplicationException, ODataLibraryException {
        QueryOptions queryOptions = new URIQueryOptions(uriInfo, rootUrl);

        EntityCollectionResponse entityCollectionResponse = requestHandler
                .handleEntityCollectionRequest(new SensorThingsRequest(uriInfo.getUriResourceParts(), queryOptions));

        InputStream serializedContent = createResponseContent(serviceMetadata, entityCollectionResponse, queryOptions);

        // configure the response object: set the body, headers and status code
        response.setContent(serializedContent);
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.JSON_NO_METADATA.toContentTypeString());
    }

    public InputStream createResponseContent(ServiceMetadata serviceMetadata,
                                             EntityCollectionResponse response,
                                             QueryOptions queryOptions) throws SerializerException {
        EdmEntityType edmEntityType = response.getEntitySet().getEntityType();

        ContextURL.Builder contextUrlBuilder = ContextURL.with().entitySet(response.getEntitySet());
        contextUrlBuilder.selectList(queryOptionsHandler.getSelectListFromSelectOption(
                edmEntityType, queryOptions.getExpandOption(), queryOptions.getSelectOption()));
        ContextURL contextUrl = contextUrlBuilder.build();

        final String id = queryOptions.getBaseURI() + "/" + response.getEntitySet().getName();

        EntityCollectionSerializerOptions opts
                = EntityCollectionSerializerOptions
                .with()
                .id(id)
                .contextURL(contextUrl)
                .select(queryOptions.getSelectOption())
                .expand(queryOptions.getExpandOption())
                .count(queryOptions.getCountOption())
                .build();
        SerializerResult serializerResult = serializer.entityCollection(serviceMetadata,
                edmEntityType,
                response.getEntityCollection(),
                opts);
        InputStream serializedContent = serializerResult.getContent();

        return serializedContent;
    }
}
