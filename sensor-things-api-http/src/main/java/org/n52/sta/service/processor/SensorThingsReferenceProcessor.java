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
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.ReferenceProcessor;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.ReferenceSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.n52.sta.service.handler.AbstractEntityRequestHandler;
import org.n52.sta.service.query.QueryOptions;
import org.n52.sta.service.query.URIQueryOptions;
import org.n52.sta.service.request.SensorThingsRequest;
import org.n52.sta.service.response.EntityResponse;
import org.n52.sta.service.serializer.SensorThingsSerializer;
import org.n52.sta.utils.EntityAnnotator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.List;

/**
 * @author <a href="mailto:s.drost@52north.org">Sebastian Drost</a>
 */
@Component
public class SensorThingsReferenceProcessor implements ReferenceProcessor {

    private final AbstractEntityRequestHandler<SensorThingsRequest, EntityResponse> requestHandler;
    private final EntityAnnotator entityAnnotator;
    private final String rootUrl;
    private final String NOT_SUPPORTED = "Not supported yet.";

    private OData odata;
    private ServiceMetadata serviceMetadata;
    private ODataSerializer serializer;

    public SensorThingsReferenceProcessor(
            AbstractEntityRequestHandler<SensorThingsRequest, EntityResponse> requestHandler,
            EntityAnnotator entityAnnotator,
            @Value("${server.rootUrl}") String rootUrl) {
        this.requestHandler = requestHandler;
        this.entityAnnotator = entityAnnotator;
        this.rootUrl = rootUrl;
        this.serializer = new SensorThingsSerializer(ContentType.JSON_NO_METADATA);
    }

    @Override
    public void init(OData odata, ServiceMetadata serviceMetadata) {
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readReference(ODataRequest request,
                              ODataResponse response,
                              UriInfo uriInfo,
                              ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        QueryOptions options = new URIQueryOptions(uriInfo, rootUrl);
        List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
        EntityResponse entityResponse =
                requestHandler.handleEntityRequest(
                        new SensorThingsRequest(resourcePaths.subList(0, resourcePaths.size() - 1), options));

        InputStream serializedContent = createResponseContent(entityResponse, request.getRawBaseUri(), options);

        // configure the response object: set the body, headers and status code
        response.setContent(serializedContent);
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.JSON_NO_METADATA.toContentTypeString());
    }

    @Override
    public void createReference(ODataRequest request,
                                ODataResponse response,
                                UriInfo uriInfo,
                                ContentType requestFormat) {
        throw new UnsupportedOperationException(NOT_SUPPORTED);
    }

    @Override
    public void updateReference(ODataRequest request,
                                ODataResponse response,
                                UriInfo uriInfo,
                                ContentType requestFormat) {
        throw new UnsupportedOperationException(NOT_SUPPORTED);
    }

    @Override
    public void deleteReference(ODataRequest request, ODataResponse response, UriInfo uriInfo) {
        throw new UnsupportedOperationException(NOT_SUPPORTED);
    }

    private InputStream createResponseContent(EntityResponse response,
                                              String rawBaseUri,
                                              QueryOptions options) throws SerializerException {

        // annotate the entity
        entityAnnotator.annotateEntity(response.getEntity(),
                response.getEntitySet().getEntityType(), rawBaseUri, options.getSelectOption());

        // and serialize the content: transform from the EntitySet object to InputStream
        ContextURL contextUrl = ContextURL.with()
                .entitySet(response.getEntitySet())
                .suffix(ContextURL.Suffix.REFERENCE)
                .build();

        ReferenceSerializerOptions opts = ReferenceSerializerOptions.with()
                .contextURL(contextUrl)
                .build();

        SerializerResult serializerResult =
                serializer.reference(serviceMetadata, response.getEntitySet(), response.getEntity(), opts);
        InputStream serializedContent = serializerResult.getContent();

        return serializedContent;
    }
}
