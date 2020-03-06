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
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.deserializer.DeserializerResult;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.uri.UriHelperImpl;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.n52.sta.service.handler.AbstractEntityRequestHandler;
import org.n52.sta.service.query.QueryOptions;
import org.n52.sta.service.query.QueryOptionsHandler;
import org.n52.sta.service.query.URIQueryOptions;
import org.n52.sta.service.request.SensorThingsRequest;
import org.n52.sta.service.response.EntityResponse;
import org.n52.sta.service.serializer.SensorThingsSerializer;
import org.n52.sta.utils.CrudHelper;
import org.n52.sta.utils.EntityAnnotator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

/**
 * @author <a href="mailto:s.drost@52north.org">Sebastian Drost</a>
 */
@Component
public class SensorThingsEntityProcessor implements EntityProcessor {

    private final AbstractEntityRequestHandler<SensorThingsRequest, EntityResponse> requestHandler;
    private final QueryOptionsHandler queryOptionsHandler;
    private final EntityAnnotator entityAnnotator;
    private final CrudHelper crudHelper;
    private final String rootUrl;
    private final ODataSerializer serializer;

    //private    IMqttClient publisher ;
    private ServiceMetadata serviceMetadata;
    MqttMessage msg;

    public SensorThingsEntityProcessor(AbstractEntityRequestHandler<SensorThingsRequest,
            EntityResponse> requestHandler,
                                       QueryOptionsHandler queryOptionsHandler,
                                       EntityAnnotator entityAnnotator,
                                       CrudHelper crudHelper,
                                       @Value("${server.rootUrl}") String rootUrl) {
        this.requestHandler = requestHandler;
        this.queryOptionsHandler = queryOptionsHandler;
        this.entityAnnotator = entityAnnotator;
        this.crudHelper = crudHelper;
        this.rootUrl = rootUrl;
        this.serializer = new SensorThingsSerializer(ContentType.JSON_NO_METADATA);
        this.queryOptionsHandler.setUriHelper(new UriHelperImpl());
        String publisherId = UUID.randomUUID().toString();
        /*try {
            this.publisher = new MqttClient("tcp://localhost:1883",publisherId);
        } catch (MqttException e) {
            e.printStackTrace();
        }*/
    }

    @Override
    public void init(OData odata, ServiceMetadata sm) {
        this.serviceMetadata = sm;
    }

    @Override
    public void readEntity(ODataRequest request, ODataResponse response,
                           UriInfo uriInfo,
                           ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        QueryOptions queryOptions = new URIQueryOptions(uriInfo, rootUrl);
        EntityResponse entityResponse =
                requestHandler.handleEntityRequest(new SensorThingsRequest(uriInfo.getUriResourceParts(),
                        queryOptions));

        InputStream serializedContent = createResponseContent(serviceMetadata, entityResponse, queryOptions);

        // configure the response object: set the body, headers and status code
        response.setContent(serializedContent);
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.JSON_NO_METADATA.toContentTypeString());
    }

    @Override
    public void createEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat,
                             ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        QueryOptions queryOptions = new URIQueryOptions(uriInfo, rootUrl);
        EntityResponse entityResponse;

       /* this.msg = new MqttMessage();
        msg.setQos(1);
        msg.setRetained(true);
        try {
            publisher.connect();
            publisher.publish("Things",request.getBody().rea,1,true);
        } catch (MqttException e) {
            e.printStackTrace();
        }*/
        DeserializerResult deserializeRequestBody = crudHelper.deserializeRequestBody(request.getBody(), uriInfo);
        if (deserializeRequestBody.getEntity() != null) {
            entityResponse = crudHelper.getCrudEntityHanlder(uriInfo)
                    .handleCreateEntityRequest(deserializeRequestBody.getEntity(), uriInfo.getUriResourceParts());

            entityAnnotator.annotateEntity(entityResponse.getEntity(), entityResponse.getEntitySet().getEntityType(),
                    queryOptions.getBaseURI(), queryOptions.getSelectOption());
            InputStream serializedContent = createResponseContent(serviceMetadata, entityResponse, queryOptions);
            InputStream serializedContent2 = createResponseContent(serviceMetadata, entityResponse, queryOptions);
            //creating an InputStreamReader object
            InputStreamReader isReader = new InputStreamReader(serializedContent);
            //Creating a BufferedReader object

            BufferedReader reader = new BufferedReader(isReader);
            StringBuffer sb = new StringBuffer();
            String str = null;
            while(true){
                try {
                    if (!((str = reader.readLine())!= null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                sb.append(str);
            }
            System.out.println("================== :::  " + sb.toString());
            String publisherId = UUID.randomUUID().toString();

            /*try {
                String topic = URLDecoder.decode(request.getRawODataPath(), StandardCharsets.UTF_8.toString());
                System.out.println(topic.substring(1, topic.length()));
                publisher.connect();
                publisher.publish(topic.substring(1, topic.length()), sb.toString().getBytes(),1,true);
            } catch (MqttException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }*/

            // configure the response object: set the body, headers and status code
            response.setContent(serializedContent2);
            response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
            response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.APPLICATION_JSON.toContentTypeString());
            response.setHeader(HttpHeader.LOCATION, entityResponse.getEntity().getSelfLink().getHref());
        } else {
            response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.JSON_NO_METADATA.toContentTypeString());
        }
    }

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat,
                             ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        if (HttpMethod.PUT.equals(request.getMethod())) {
            throw new ODataApplicationException("Http PUT is not yet supported!",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.getDefault());
        }
        QueryOptions queryOptions = new URIQueryOptions(uriInfo, rootUrl);
        EntityResponse entityResponse;
        DeserializerResult deserializeRequestBody = crudHelper.deserializeRequestBody(request.getBody(), uriInfo);
        if (deserializeRequestBody.getEntity() != null) {
            entityResponse = crudHelper.getCrudEntityHanlder(uriInfo).handleUpdateEntityRequest(
                    deserializeRequestBody.getEntity(), request.getMethod(), uriInfo.getUriResourceParts());

            entityAnnotator.annotateEntity(
                    entityResponse.getEntity(),
                    entityResponse.getEntitySet().getEntityType(),
                    queryOptions.getBaseURI(),
                    queryOptions.getSelectOption());
            InputStream serializedContent = createResponseContent(serviceMetadata, entityResponse, queryOptions);
            // configure the response object: set the body, headers and status code
            response.setContent(serializedContent);
            response.setStatusCode(HttpStatusCode.OK.getStatusCode());
            response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.APPLICATION_JSON.toContentTypeString());
        } else {
            response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.JSON_NO_METADATA.toContentTypeString());
        }
    }

    @Override
    public void deleteEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {
        QueryOptions queryOptions = new URIQueryOptions(uriInfo, rootUrl);
        EntityResponse entityResponse = new EntityResponse();
        entityResponse = crudHelper.getCrudEntityHanlder(uriInfo)
                .handleDeleteEntityRequest(uriInfo.getUriResourceParts());
        entityAnnotator.annotateEntity(entityResponse.getEntity(),
                entityResponse.getEntitySet().getEntityType(),
                queryOptions.getBaseURI(),
                queryOptions.getSelectOption());
        // InputStream serializedContent = createResponseContent(serviceMetadata, entityResponse, queryOptions)
        // configure the response object: set the body, headers and status code
        // response.setContent(serializedContent);
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.JSON_NO_METADATA.toContentTypeString());
    }

    private InputStream createResponseContent(ServiceMetadata serviceMetadata,
                                              EntityResponse response,
                                              QueryOptions queryOptions) throws SerializerException {
        EdmEntityType edmEntityType = response.getEntitySet().getEntityType();

        ContextURL.Builder contextUrlBuilder = ContextURL.with()
                .entitySet(response.getEntitySet())
                .suffix(ContextURL.Suffix.ENTITY);
        contextUrlBuilder.selectList(queryOptionsHandler.getSelectListFromSelectOption(
                edmEntityType, queryOptions.getExpandOption(), queryOptions.getSelectOption()));
        ContextURL contextUrl = contextUrlBuilder.build();

        EntitySerializerOptions opts = EntitySerializerOptions.with()
                .contextURL(contextUrl)
                .select(queryOptions.getSelectOption())
                .expand(queryOptions.getExpandOption())
                .build();

        SerializerResult serializerResult = serializer.entity(
                serviceMetadata,
                response.getEntitySet().getEntityType(),
                response.getEntity(),
                opts);
        InputStream serializedContent = serializerResult.getContent();

        return serializedContent;
    }
}
