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
package org.n52.sta.service.serializer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityIterator;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Linked;
import org.apache.olingo.commons.api.data.Operation;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.edm.geo.ComposedGeospatial;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.GeospatialCollection;
import org.apache.olingo.commons.api.edm.geo.LineString;
import org.apache.olingo.commons.api.edm.geo.MultiLineString;
import org.apache.olingo.commons.api.edm.geo.MultiPoint;
import org.apache.olingo.commons.api.edm.geo.MultiPolygon;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.Polygon;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.serializer.ComplexSerializerOptions;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.PrimitiveSerializerOptions;
import org.apache.olingo.server.api.serializer.ReferenceCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ReferenceSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.serializer.SerializerStreamResult;
import org.apache.olingo.server.api.uri.UriHelper;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.LevelsExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.ODataWritableContent;
import org.apache.olingo.server.core.serializer.AbstractODataSerializer;
import org.apache.olingo.server.core.serializer.SerializerResultImpl;
import org.apache.olingo.server.core.serializer.json.ODataErrorSerializer;
import org.apache.olingo.server.core.serializer.utils.CircleStreamBuffer;
import org.apache.olingo.server.core.serializer.utils.ContentTypeHelper;
import org.apache.olingo.server.core.serializer.utils.ContextURLBuilder;
import org.apache.olingo.server.core.serializer.utils.ExpandSelectHelper;
import org.apache.olingo.server.core.uri.UriHelperImpl;
import org.apache.olingo.server.core.uri.queryoption.ExpandOptionImpl;
import org.n52.sta.edm.provider.SensorThingsEdmConstants;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


//CHECKSTYLE:OFF
public class SensorThingsSerializer extends AbstractODataSerializer {

    private static final Map<Geospatial.Type, String> geoValueTypeToJsonName;

    static {
        Map<Geospatial.Type, String> temp = new EnumMap<>(Geospatial.Type.class);
        temp.put(Geospatial.Type.POINT, Constants.ELEM_POINT);
        temp.put(Geospatial.Type.MULTIPOINT, Constants.ELEM_MULTIPOINT);
        temp.put(Geospatial.Type.LINESTRING, Constants.ELEM_LINESTRING);
        temp.put(Geospatial.Type.MULTILINESTRING, "MultiLineString");
        temp.put(Geospatial.Type.POLYGON, Constants.ELEM_POLYGON);
        temp.put(Geospatial.Type.MULTIPOLYGON, "MultiPolygon");
        temp.put(Geospatial.Type.GEOSPATIALCOLLECTION, "GeometryCollection");
        geoValueTypeToJsonName = Collections.unmodifiableMap(temp);
    }

    private final boolean isIEEE754Compatible;
    private final boolean isODataMetadataNone;
    private final boolean isODataMetadataFull;

    public SensorThingsSerializer(final ContentType contentType) {
        isIEEE754Compatible = ContentTypeHelper.isODataIEEE754Compatible(contentType);
        isODataMetadataNone = ContentTypeHelper.isODataMetadataNone(contentType);
        isODataMetadataFull = ContentTypeHelper.isODataMetadataFull(contentType);
    }

    @Override
    public SerializerResult serviceDocument(final ServiceMetadata metadata, final String serviceRoot)
            throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;

        try {
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            new SensorThingsServiceDocumentSerializer(metadata, serviceRoot, isODataMetadataNone).writeServiceDocument(json);

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult metadataDocument(final ServiceMetadata serviceMetadata) throws SerializerException {
        throw new SerializerException("Metadata in JSON format not supported!",
                SerializerException.MessageKeys.JSON_METADATA);
    }

    @Override
    public SerializerResult error(final ODataServerError error) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            new ODataErrorSerializer().writeErrorDocument(json, error);

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult entityCollection(final ServiceMetadata metadata,
                                             final EdmEntityType entityType, final AbstractEntityCollection entitySet,
                                             final EntityCollectionSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        boolean pagination = false;
        try {
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            json.writeStartObject();

            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            String name = contextURL == null ? null : contextURL.getEntitySetOrSingletonOrType();
            writeContextURL(contextURL, json);

            writeMetadataETag(metadata, json);

            if (options != null && options.getCount() != null && options.getCount().getValue()) {
                writeInlineCount("", entitySet.getCount(), json);
            }
            writeOperations(entitySet.getOperations(), json);
            json.writeFieldName(Constants.VALUE);
            if (options == null) {
                writeEntitySet(metadata, entityType, entitySet, null, null, null, false, null, name, json);
            } else {
                writeEntitySet(metadata, entityType, entitySet,
                        options.getExpand(), null, options.getSelect(), options.getWriteOnlyReferences(), null, name, json);
            }
            writeNextLink(entitySet, json, pagination);
            writeDeltaLink(entitySet, json, pagination);

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerStreamResult entityCollectionStreamed(ServiceMetadata metadata, EdmEntityType entityType,
                                                           EntityIterator entities, EntityCollectionSerializerOptions options) throws SerializerException {

        return ODataWritableContent.with(entities, entityType, this, metadata, options).build();
    }

    public void entityCollectionIntoStream(final ServiceMetadata metadata,
                                           final EdmEntityType entityType, final EntityIterator entitySet,
                                           final EntityCollectionSerializerOptions options, final OutputStream outputStream)
            throws SerializerException {

        SerializerException cachedException;
        boolean pagination = false;
        try {
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            json.writeStartObject();

            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            writeContextURL(contextURL, json);

            writeMetadataETag(metadata, json);

            if (options != null && options.getCount() != null && options.getCount().getValue()) {
                writeInlineCount("", entitySet.getCount(), json);
            }
            json.writeFieldName(Constants.VALUE);
            String name = contextURL == null ? null : contextURL.getEntitySetOrSingletonOrType();
            if (options == null) {
                writeEntitySet(metadata, entityType, entitySet, null, null, null, false, null, name, json);
            } else {
                writeEntitySet(metadata, entityType, entitySet,
                        options.getExpand(), null, options.getSelect(), options.getWriteOnlyReferences(), null, name, json);
            }
            // next link support for streaming results
            writeNextLink(entitySet, json, pagination);

            json.close();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        }
    }

    @Override
    public SerializerResult entity(final ServiceMetadata metadata, final EdmEntityType entityType,
                                   final Entity entity, final EntitySerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            String name = contextURL == null ? null : contextURL.getEntitySetOrSingletonOrType();
            writeEntity(metadata, entityType, entity, contextURL,
                    options == null ? null : options.getExpand(),
                    null,
                    options == null ? null : options.getSelect(),
                    options == null ? false : options.getWriteOnlyReferences(),
                    null, name,
                    json);

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    ContextURL checkContextURL(final ContextURL contextURL) throws SerializerException {
        if (isODataMetadataNone) {
            return null;
        } else if (contextURL == null) {
            throw new SerializerException("ContextURL null!", SerializerException.MessageKeys.NO_CONTEXT_URL);
        }
        return contextURL;
    }

    protected void writeEntitySet(final ServiceMetadata metadata, final EdmEntityType entityType,
                                  final AbstractEntityCollection entitySet, final ExpandOption expand, Integer toDepth, final SelectOption select,
                                  final boolean onlyReference, final Set<String> ancestors, String name, final JsonGenerator json)
            throws IOException, SerializerException {
        json.writeStartArray();
        for (final Entity entity : entitySet) {
            if (onlyReference) {
                json.writeStartObject();
                json.writeStringField(SensorThingsEdmConstants.SELF_LINK_ANNOTATION, entity.getSelfLink().getHref());
                json.writeEndObject();
            } else {
                writeEntity(metadata, entityType, entity, null, expand, toDepth, select, false, ancestors, name, json);
            }
        }
        json.writeEndArray();
    }

    /**
     * Get the ascii representation of the entity id or thrown an
     * {@link SerializerException} if id is <code>null</code>.
     *
     * @param entity     the entity
     * @param entityType
     * @param name
     * @return ascii representation of the entity id
     */
    private String getEntityId(Entity entity, EdmEntityType entityType, String name) throws SerializerException {
        if (entity.getId() == null) {
            if (entity == null || entityType == null || entityType.getKeyPredicateNames() == null
                    || name == null) {
                throw new SerializerException("Entity id is null.", SerializerException.MessageKeys.MISSING_ID);
            } else {
                final UriHelper uriHelper = new UriHelperImpl();
                entity.setId(URI.create(name + '(' + uriHelper.buildKeyPredicate(entityType, entity) + ')'));
            }
        }
        return entity.getId().toASCIIString();
    }

    private boolean areKeyPredicateNamesSelected(SelectOption select, EdmEntityType type) {
        if (select == null || ExpandSelectHelper.isAll(select)) {
            return true;
        }
        final Set<String> selected = ExpandSelectHelper.getSelectedPropertyNames(select.getSelectItems());
        for (String key : type.getKeyPredicateNames()) {
            if (!selected.contains(key)) {
                return false;
            }
        }
        return true;
    }

    protected void writeEntity(final ServiceMetadata metadata, final EdmEntityType entityType, final Entity entity,
                               final ContextURL contextURL, final ExpandOption expand, Integer toDepth,
                               final SelectOption select, final boolean onlyReference, Set<String> ancestors,
                               String name, final JsonGenerator json)
            throws IOException, SerializerException {
        boolean cycle = false;
        if (expand != null) {
            if (ancestors == null) {
                ancestors = new HashSet<>();
            }
            cycle = !ancestors.add(getEntityId(entity, entityType, name));
        }
        try {
            json.writeStartObject();
            if (!isODataMetadataNone) {
                // top-level entity
                if (contextURL != null) {
                    writeContextURL(contextURL, json);
                    writeMetadataETag(metadata, json);
                }
                if (entity.getETag() != null) {
                    json.writeStringField(Constants.JSON_ETAG, entity.getETag());
                }
                if (entityType.hasStream()) {
                    if (entity.getMediaETag() != null) {
                        json.writeStringField(Constants.JSON_MEDIA_ETAG, entity.getMediaETag());
                    }
                    if (entity.getMediaContentType() != null) {
                        json.writeStringField(Constants.JSON_MEDIA_CONTENT_TYPE, entity.getMediaContentType());
                    }
                    if (entity.getMediaContentSource() != null) {
                        json.writeStringField(Constants.JSON_MEDIA_READ_LINK, entity.getMediaContentSource().toString());
                    }
                    if (entity.getMediaEditLinks() != null && !entity.getMediaEditLinks().isEmpty()) {
                        json.writeStringField(Constants.JSON_MEDIA_EDIT_LINK, entity.getMediaEditLinks().get(0).getHref());
                    }
                }
            }
            if (cycle || onlyReference) {
                json.writeStringField(SensorThingsEdmConstants.SELF_LINK_ANNOTATION, entity.getSelfLink().getHref());
            } else {
                final EdmEntityType resolvedType = resolveEntityType(metadata, entityType, entity.getType());
                if ((!isODataMetadataNone && !resolvedType.equals(entityType)) || isODataMetadataFull) {
                    json.writeStringField(Constants.JSON_TYPE, "#" + entity.getType());
                }

                final boolean all = ExpandSelectHelper.isAll(select);
                final Set<String> selected = all ? new HashSet<>()
                        : ExpandSelectHelper.getSelectedPropertyNames(select.getSelectItems());

                for (final String propertyName : entityType.getKeyPredicateNames()) {
                    if (all || selected.contains(propertyName)) {
                        if (entity.getSelfLink() != null) {
                            json.writeStringField(SensorThingsEdmConstants.SELF_LINK_ANNOTATION, entity.getSelfLink().getHref());
                        }
                        if (entityType.getKeyPredicateNames().contains(propertyName)) {
                            final EdmProperty edmProperty = entityType.getStructuralProperty(propertyName);
                            final Property property = findProperty(propertyName, entity.getProperties());
                            final Set<List<String>> selectedPaths = all || edmProperty.isPrimitive() ? null
                                    : ExpandSelectHelper.getSelectedPaths(select.getSelectItems(), propertyName);

                            json.writeFieldName(SensorThingsEdmConstants.ID_ANNOTATION);
                            writePropertyValue(metadata, edmProperty, property, selectedPaths, json);
                        }

                    }
                }

                writeProperties(metadata, resolvedType, entity.getProperties(), select, json);
                writeNavigationProperties(metadata, resolvedType, entity, expand, toDepth, ancestors, name, json);
                writeOperations(entity.getOperations(), json);
            }
            json.writeEndObject();
        } finally {
            if (expand != null && !cycle && ancestors != null) {
                ancestors.remove(getEntityId(entity, entityType, name));
            }
        }
    }

    private void writeOperations(final List<Operation> operations, final JsonGenerator json)
            throws IOException {
        if (isODataMetadataFull) {
            for (Operation operation : operations) {
                json.writeObjectFieldStart(operation.getMetadataAnchor());
                json.writeStringField(Constants.ATTR_TITLE, operation.getTitle());
                json.writeStringField(Constants.ATTR_TARGET, operation.getTarget().toASCIIString());
                json.writeEndObject();
            }
        }
    }

    protected EdmEntityType resolveEntityType(final ServiceMetadata metadata, final EdmEntityType baseType,
                                              final String derivedTypeName) throws SerializerException {
        if (derivedTypeName == null
                || baseType.getFullQualifiedName().getFullQualifiedNameAsString().equals(derivedTypeName)) {
            return baseType;
        }
        EdmEntityType derivedType = metadata.getEdm().getEntityType(new FullQualifiedName(derivedTypeName));
        if (derivedType == null) {
            throw new SerializerException("EntityType not found",
                    SerializerException.MessageKeys.UNKNOWN_TYPE, derivedTypeName);
        }
        EdmEntityType type = derivedType.getBaseType();
        while (type != null) {
            if (type.getFullQualifiedName().equals(baseType.getFullQualifiedName())) {
                return derivedType;
            }
            type = type.getBaseType();
        }
        throw new SerializerException("Wrong base type",
                SerializerException.MessageKeys.WRONG_BASE_TYPE, derivedTypeName,
                baseType.getFullQualifiedName().getFullQualifiedNameAsString());
    }

    protected EdmComplexType resolveComplexType(final ServiceMetadata metadata, final EdmComplexType baseType,
                                                final String derivedTypeName) throws SerializerException {

        String fullQualifiedName = baseType.getFullQualifiedName().getFullQualifiedNameAsString();
        if (derivedTypeName == null
                || fullQualifiedName.equals(derivedTypeName)) {
            return baseType;
        }
        EdmComplexType derivedType = metadata.getEdm().getComplexType(new FullQualifiedName(derivedTypeName));
        if (derivedType == null) {
            throw new SerializerException("Complex Type not found",
                    SerializerException.MessageKeys.UNKNOWN_TYPE, derivedTypeName);
        }
        EdmComplexType type = derivedType.getBaseType();
        while (type != null) {
            if (type.getFullQualifiedName().equals(baseType.getFullQualifiedName())) {
                return derivedType;
            }
            type = type.getBaseType();
        }
        throw new SerializerException("Wrong base type",
                SerializerException.MessageKeys.WRONG_BASE_TYPE, derivedTypeName,
                baseType.getFullQualifiedName().getFullQualifiedNameAsString());
    }

    protected void writeProperties(final ServiceMetadata metadata, final EdmStructuredType type,
                                   final List<Property> properties,
                                   final SelectOption select, final JsonGenerator json)
            throws IOException, SerializerException {
        final boolean all = ExpandSelectHelper.isAll(select);
        final Set<String> selected = all ? new HashSet<>()
                : ExpandSelectHelper.getSelectedPropertyNames(select.getSelectItems());
        for (final String propertyName : type.getPropertyNames()) {
            if (all || selected.contains(propertyName)) {
                if (!(type instanceof EdmEntityType)
                        || ((type instanceof EdmEntityType)
                        && !((EdmEntityType) type).getKeyPredicateNames().contains(propertyName))) {
                    final EdmProperty edmProperty = type.getStructuralProperty(propertyName);
                    final Property property = findProperty(propertyName, properties);
                    final Set<List<String>> selectedPaths = all || edmProperty.isPrimitive() ? null
                            : ExpandSelectHelper.getSelectedPaths(select.getSelectItems(), propertyName);
                    writeProperty(metadata, edmProperty, property, selectedPaths, json);
                }

            }
        }
    }

    protected void writeNavigationProperties(final ServiceMetadata metadata,
                                             final EdmStructuredType type, final Linked linked, final ExpandOption expand, final Integer toDepth,
                                             final Set<String> ancestors, final String name, final JsonGenerator json)
            throws SerializerException, IOException {
        for (final String propertyName : type.getNavigationPropertyNames()) {
            final Link navigationLink = linked.getNavigationLink(propertyName);
            if (navigationLink != null) {
                json.writeStringField(propertyName + SensorThingsEdmConstants.NAVIGATION_LINK_ANNOTATION, navigationLink.getHref());
            }
            final Link associationLink = linked.getAssociationLink(propertyName);
            if (associationLink != null) {
                json.writeStringField(propertyName + Constants.JSON_ASSOCIATION_LINK, associationLink.getHref());
            }
        }
        if ((toDepth != null && toDepth > 1) || (toDepth == null && ExpandSelectHelper.hasExpand(expand))) {
            final ExpandItem expandAll = ExpandSelectHelper.getExpandAll(expand);
            for (final String propertyName : type.getNavigationPropertyNames()) {
                final ExpandItem innerOptions = ExpandSelectHelper.getExpandItem(expand.getExpandItems(), propertyName);
                if (innerOptions != null || expandAll != null || toDepth != null) {
                    Integer levels = null;
                    final EdmNavigationProperty property = type.getNavigationProperty(propertyName);
                    final Link navigationLink = linked.getNavigationLink(property.getName());
                    ExpandOption childExpand = null;
                    LevelsExpandOption levelsOption = null;
                    if (innerOptions != null) {
                        levelsOption = innerOptions.getLevelsOption();
                        childExpand = levelsOption == null ? innerOptions.getExpandOption() : new ExpandOptionImpl().addExpandItem(
                                innerOptions);
                    } else if (expandAll != null) {
                        levels = 1;
                        levelsOption = expandAll.getLevelsOption();
                        childExpand = new ExpandOptionImpl().addExpandItem(expandAll);
                    }

                    if (levelsOption != null) {
                        levels = levelsOption.isMax() ? Integer.MAX_VALUE : levelsOption.getValue();
                    }
                    if (toDepth != null) {
                        levels = toDepth - 1;
                        childExpand = expand;
                    }

                    writeExpandedNavigationProperty(metadata, property, navigationLink,
                            childExpand, levels,
                            innerOptions == null ? null : innerOptions.getSelectOption(),
                            innerOptions == null ? null : innerOptions.getCountOption(),
                            innerOptions == null ? false : innerOptions.hasCountPath(),
                            innerOptions == null ? false : innerOptions.isRef(),
                            ancestors, name,
                            json);
                }
            }
        }
    }

    protected void writeExpandedNavigationProperty(
            final ServiceMetadata metadata, final EdmNavigationProperty property,
            final Link navigationLink, final ExpandOption innerExpand,
            Integer toDepth, final SelectOption innerSelect, final CountOption innerCount,
            final boolean writeOnlyCount, final boolean writeOnlyRef, final Set<String> ancestors,
            String name, final JsonGenerator json) throws IOException, SerializerException {

        if (property.isCollection()) {
            if (writeOnlyCount) {
                if (navigationLink == null || navigationLink.getInlineEntitySet() == null) {
                    writeInlineCount(property.getName(), 0, json);
                } else {
                    writeInlineCount(property.getName(), navigationLink.getInlineEntitySet().getCount(), json);
                }
            } else {
                if (navigationLink == null || navigationLink.getInlineEntitySet() == null) {
                    if (innerCount != null && innerCount.getValue()) {
                        writeInlineCount(property.getName(), 0, json);
                    }
                    json.writeFieldName(property.getName());
                    json.writeStartArray();
                    json.writeEndArray();
                } else {
                    if (innerCount != null && innerCount.getValue()) {
                        writeInlineCount(property.getName(), navigationLink.getInlineEntitySet().getCount(), json);
                    }
                    json.writeFieldName(property.getName());
                    writeEntitySet(metadata, property.getType(), navigationLink.getInlineEntitySet(), innerExpand, toDepth,
                            innerSelect, writeOnlyRef, ancestors, name, json);
                }
            }
        } else {
            json.writeFieldName(property.getName());
            if (navigationLink == null || navigationLink.getInlineEntity() == null) {
                json.writeNull();
            } else {
                writeEntity(metadata, property.getType(), navigationLink.getInlineEntity(), null,
                        innerExpand, toDepth, innerSelect, writeOnlyRef, ancestors, name, json);
            }
        }
    }

    private boolean isStreamProperty(EdmProperty edmProperty) {
        final EdmType type = edmProperty.getType();
        return (edmProperty.isPrimitive() && type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Stream));
    }

    protected void writeProperty(final ServiceMetadata metadata,
                                 final EdmProperty edmProperty, final Property property,
                                 final Set<List<String>> selectedPaths, final JsonGenerator json)
            throws IOException, SerializerException {
        boolean isStreamProperty = isStreamProperty(edmProperty);
        writePropertyType(edmProperty, json);
        if (!isStreamProperty) {
            json.writeFieldName(edmProperty.getName());
        }
        if (property == null || property.isNull()) {
            if (edmProperty.isNullable() == Boolean.FALSE) {
                throw new SerializerException("Non-nullable property not present!",
                        SerializerException.MessageKeys.MISSING_PROPERTY, edmProperty.getName());
            } else {
                if (!isStreamProperty) {
                    if (edmProperty.isCollection()) {
                        json.writeStartArray();
                        json.writeEndArray();
                    } else {
                        json.writeNull();
                    }
                }
            }
        } else {
            writePropertyValue(metadata, edmProperty, property, selectedPaths, json);
        }
    }

    private void writePropertyType(final EdmProperty edmProperty, JsonGenerator json)
            throws SerializerException, IOException {
        if (!isODataMetadataFull) {
            return;
        }
        String typeName = edmProperty.getName() + Constants.JSON_TYPE;
        final EdmType type = edmProperty.getType();
        if (type.getKind() == EdmTypeKind.ENUM || type.getKind() == EdmTypeKind.DEFINITION) {
            if (edmProperty.isCollection()) {
                json.writeStringField(typeName,
                        "#Collection(" + type.getFullQualifiedName().getFullQualifiedNameAsString() + ")");
            } else {
                json.writeStringField(typeName, "#" + type.getFullQualifiedName().getFullQualifiedNameAsString());
            }
        } else if (edmProperty.isPrimitive()) {
            if (edmProperty.isCollection()) {
                json.writeStringField(typeName, "#Collection(" + type.getFullQualifiedName().getName() + ")");
            } else {
                // exclude the properties that can be heuristically determined
                if (type != EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Boolean)
                        && type != EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Double)
                        && type != EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.String)) {
                    json.writeStringField(typeName, "#" + type.getFullQualifiedName().getName());
                }
            }
        } else if (type.getKind() == EdmTypeKind.COMPLEX) {
            // non-collection case written in writeComplex method directly.
            if (edmProperty.isCollection()) {
                json.writeStringField(typeName,
                        "#Collection(" + type.getFullQualifiedName().getFullQualifiedNameAsString() + ")");
            }
        } else {
            throw new SerializerException("Property type not yet supported!",
                    SerializerException.MessageKeys.UNSUPPORTED_PROPERTY_TYPE, edmProperty.getName());
        }
    }

    private void writePropertyValue(final ServiceMetadata metadata, final EdmProperty edmProperty,
                                    final Property property, final Set<List<String>> selectedPaths, final JsonGenerator json)
            throws IOException, SerializerException {
        final EdmType type = edmProperty.getType();
        try {
            if (edmProperty.isPrimitive()
                    || type.getKind() == EdmTypeKind.ENUM || type.getKind() == EdmTypeKind.DEFINITION) {
                if (edmProperty.isCollection()) {
                    writePrimitiveCollection((EdmPrimitiveType) type, property,
                            edmProperty.isNullable(), edmProperty.getMaxLength(),
                            edmProperty.getPrecision(), edmProperty.getScale(), edmProperty.isUnicode(), json);
                } else {
                    writePrimitive((EdmPrimitiveType) type, property,
                            edmProperty.isNullable(), edmProperty.getMaxLength(),
                            edmProperty.getPrecision(), edmProperty.getScale(), edmProperty.isUnicode(), json);
                }
            } else if (property.isComplex()) {
                if (edmProperty.isCollection()) {
                    writeComplexCollection(metadata, (EdmComplexType) type, property, selectedPaths, json);
                } else if (((EdmComplexType) type).isOpenType()
                        && (((EdmComplexType) type).getPropertyNames() == null
                        || ((EdmComplexType) type).getPropertyNames().isEmpty())) {
                    writeOpenTypeComplexValue(property, json);
                } else {
                    writeComplex(metadata, (EdmComplexType) type, property, selectedPaths, json);
                }
            } else {
                throw new SerializerException("Property type not yet supported!",
                        SerializerException.MessageKeys.UNSUPPORTED_PROPERTY_TYPE, edmProperty.getName());
            }
        } catch (final EdmPrimitiveTypeException e) {
            throw new SerializerException("Wrong value for property!", e,
                    SerializerException.MessageKeys.WRONG_PROPERTY_VALUE,
                    edmProperty.getName(), property.getValue().toString());
        }
    }

    private void writeComplex(final ServiceMetadata metadata, final EdmComplexType type,
                              final Property property, final Set<List<String>> selectedPaths, final JsonGenerator json)
            throws IOException, SerializerException {
        json.writeStartObject();
        String derivedName = property.getType();
        EdmComplexType resolvedType = null;
        if (!type.getFullQualifiedName().getFullQualifiedNameAsString().
                equals(derivedName)) {
            if (type.getBaseType() != null
                    && type.getBaseType().getFullQualifiedName().getFullQualifiedNameAsString().
                    equals(derivedName)) {
                resolvedType = resolveComplexType(metadata, type.getBaseType(),
                        type.getFullQualifiedName().getFullQualifiedNameAsString());
            } else {
                resolvedType = resolveComplexType(metadata, type, derivedName);
            }
        } else {
            resolvedType = resolveComplexType(metadata, type, derivedName);
        }
        if (!isODataMetadataNone && !resolvedType.equals(type) || isODataMetadataFull) {
            json.writeStringField(Constants.JSON_TYPE, "#"
                    + resolvedType.getFullQualifiedName().getFullQualifiedNameAsString());
        }
        writeComplexValue(metadata, resolvedType, property.asComplex().getValue(), selectedPaths,
                json);
        json.writeEndObject();
    }

    private void writeOpenTypeComplex(ServiceMetadata metadata, EdmComplexType type, Property property, JsonGenerator json) throws SerializerException, IOException {
        String derivedName = property.getType();
        EdmComplexType resolvedType = null;
        if (!type.getFullQualifiedName().getFullQualifiedNameAsString().
                equals(derivedName)) {
            if (type.getBaseType() != null
                    && type.getBaseType().getFullQualifiedName().getFullQualifiedNameAsString().
                    equals(derivedName)) {
                resolvedType = resolveComplexType(metadata, type.getBaseType(),
                        type.getFullQualifiedName().getFullQualifiedNameAsString());
            } else {
                resolvedType = resolveComplexType(metadata, type, derivedName);
            }
        } else {
            resolvedType = resolveComplexType(metadata, type, derivedName);
        }
        if (!isODataMetadataNone && !resolvedType.equals(type) || isODataMetadataFull) {
            json.writeStringField(Constants.JSON_TYPE, "#"
                    + resolvedType.getFullQualifiedName().getFullQualifiedNameAsString());
        }
        writeOpenTypeComplexValue(property, json);
    }

    private void writeOpenTypeComplexValue(Property property, JsonGenerator json) throws SerializerException, IOException {
        if (property.getValue() instanceof ObjectNode) {
            ObjectNode value = (ObjectNode) property.getValue();
            json.writeRawValue(value.toString());
        } else {
            throw new SerializerException("Property type value not yet supported!",
                    SerializerException.MessageKeys.UNSUPPORTED_PROPERTY_TYPE, property.getName());
        }
    }

    private void writePrimitiveCollection(final EdmPrimitiveType type, final Property property,
                                          final Boolean isNullable, final Integer maxLength, final Integer precision, final Integer scale,
                                          final Boolean isUnicode, final JsonGenerator json)
            throws IOException, SerializerException {
        json.writeStartArray();
        for (Object value : property.asCollection()) {
            switch (property.getValueType()) {
                case COLLECTION_PRIMITIVE:
                case COLLECTION_ENUM:
                case COLLECTION_GEOSPATIAL:
                    try {
                        writePrimitiveValue(property.getName(), type, value, isNullable,
                                maxLength, precision, scale, isUnicode, json);
                    } catch (EdmPrimitiveTypeException e) {
                        throw new SerializerException("Wrong value for property!", e,
                                SerializerException.MessageKeys.WRONG_PROPERTY_VALUE,
                                property.getName(), property.getValue().toString());
                    }
                    break;
                default:
                    throw new SerializerException("Property type not yet supported!",
                            SerializerException.MessageKeys.UNSUPPORTED_PROPERTY_TYPE, property.getName());
            }
        }
        json.writeEndArray();
    }

    private void writeComplexCollection(final ServiceMetadata metadata, final EdmComplexType type,
                                        final Property property,
                                        final Set<List<String>> selectedPaths, final JsonGenerator json)
            throws IOException, SerializerException {
        json.writeStartArray();
        EdmComplexType derivedType = type;
        for (Object value : property.asCollection()) {

            switch (property.getValueType()) {
                case COLLECTION_COMPLEX:
                    derivedType = ((ComplexValue) value).getTypeName() != null ? metadata.getEdm().getComplexType(new FullQualifiedName(((ComplexValue) value).getTypeName())) : type;
                    if (derivedType.isOpenType()
                            && (derivedType.getPropertyNames() == null
                            || derivedType.getPropertyNames().isEmpty())) {
                        for (Property p : ((ComplexValue) value).getValue()) {
                            writeOpenTypeComplex(metadata, derivedType, p, json);
                        }

                    } else {
                        json.writeStartObject();
                        if (isODataMetadataFull || (!isODataMetadataNone && !derivedType.equals(type))) {
                            json.writeStringField(Constants.JSON_TYPE, "#"
                                    + derivedType.getFullQualifiedName().getFullQualifiedNameAsString());
                        }
                        writeComplexValue(metadata, derivedType, ((ComplexValue) value).getValue(), selectedPaths, json);
                        json.writeEndObject();
                    }

                    break;
                default:
                    throw new SerializerException("Property type not yet supported!",
                            SerializerException.MessageKeys.UNSUPPORTED_PROPERTY_TYPE, property.getName());
            }
        }
        json.writeEndArray();
    }

    private void writePrimitive(final EdmPrimitiveType type, final Property property,
                                final Boolean isNullable, final Integer maxLength, final Integer precision, final Integer scale,
                                final Boolean isUnicode, final JsonGenerator json)
            throws EdmPrimitiveTypeException, IOException, SerializerException {
        if (property.isPrimitive()) {
            writePrimitiveValue(property.getName(), type, property.asPrimitive(),
                    isNullable, maxLength, precision, scale, isUnicode, json);
        } else if (property.isGeospatial()) {
            writeGeoValue(property.getName(), type, property.asGeospatial(), isNullable, json);
        } else if (property.isEnum()) {
            writePrimitiveValue(property.getName(), type, property.asEnum(),
                    isNullable, maxLength, precision, scale, isUnicode, json);
        } else {
            throw new SerializerException("Inconsistent property type!",
                    SerializerException.MessageKeys.INCONSISTENT_PROPERTY_TYPE, property.getName());
        }
    }

    protected void writePrimitiveValue(final String name, final EdmPrimitiveType type, final Object primitiveValue,
                                       final Boolean isNullable, final Integer maxLength, final Integer precision, final Integer scale,
                                       final Boolean isUnicode, final JsonGenerator json) throws EdmPrimitiveTypeException, IOException {
        final String value = type.valueToString(primitiveValue,
                isNullable, maxLength, precision, scale, isUnicode);
        if (value == null) {
            json.writeNull();
        } else if (type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Boolean)) {
            json.writeBoolean(Boolean.parseBoolean(value));
        } else if (type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Byte)
                || type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Double)
                || type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Int16)
                || type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Int32)
                || type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.SByte)
                || type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Single)
                || (type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Decimal)
                || type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Int64))
                && !isIEEE754Compatible) {
            json.writeNumber(value);
        } else if (type == EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Stream)) {
            if (primitiveValue instanceof Link) {
                Link stream = (Link) primitiveValue;
                if (!isODataMetadataNone) {
                    if (stream.getMediaETag() != null) {
                        json.writeStringField(name + Constants.JSON_MEDIA_ETAG, stream.getMediaETag());
                    }
                    if (stream.getType() != null) {
                        json.writeStringField(name + Constants.JSON_MEDIA_CONTENT_TYPE, stream.getType());
                    }
                }
                if (isODataMetadataFull) {
                    if (stream.getRel() != null && stream.getRel().equals(Constants.NS_MEDIA_READ_LINK_REL)) {
                        json.writeStringField(name + Constants.JSON_MEDIA_READ_LINK, stream.getHref());
                    }
                    if (stream.getRel() == null || stream.getRel().equals(Constants.NS_MEDIA_EDIT_LINK_REL)) {
                        json.writeStringField(name + Constants.JSON_MEDIA_EDIT_LINK, stream.getHref());
                    }
                }
            }
        } else {
            json.writeString(value);
        }
    }

    /**
     * Writes a geospatial value following the GeoJSON specification defined in
     * RFC 7946.
     *
     * @param name       The name
     * @param type       The type
     * @param geoValue   The geo value
     * @param isNullable Flag if it is nullable
     * @param json       The json representation
     * @throws EdmPrimitiveTypeException if an error occurs
     * @throws IOException               if an error occurs
     * @throws SerializerException       if an error occurs
     */
    protected void writeGeoValue(final String name, final EdmPrimitiveType type, final Geospatial geoValue,
                                 final Boolean isNullable, JsonGenerator json)
            throws EdmPrimitiveTypeException, IOException, SerializerException {
        if (geoValue == null) {
            if (isNullable == null || isNullable) {
                json.writeNull();
            } else {
                throw new EdmPrimitiveTypeException("The literal 'null' is not allowed.");
            }
        } else {
            if (!type.getDefaultType().isAssignableFrom(geoValue.getClass())) {
                throw new EdmPrimitiveTypeException("The value type " + geoValue.getClass() + " is not supported.");
            }
            if (geoValue.getSrid() != null && geoValue.getSrid().isNotDefault()) {
                throw new SerializerException("Non-standard SRID not supported!",
                        SerializerException.MessageKeys.WRONG_PROPERTY_VALUE, name, geoValue.toString());
            }
            json.writeStartObject();
            json.writeStringField(Constants.ATTR_TYPE, geoValueTypeToJsonName.get(geoValue.getGeoType()));
            json.writeFieldName(geoValue.getGeoType() == Geospatial.Type.GEOSPATIALCOLLECTION
                    ? Constants.JSON_GEOMETRIES
                    : Constants.JSON_COORDINATES);
            json.writeStartArray();
            switch (geoValue.getGeoType()) {
                case POINT:
                    writeGeoPoint(json, (Point) geoValue);
                    break;
                case MULTIPOINT:
                    writeGeoPoints(json, (MultiPoint) geoValue);
                    break;
                case LINESTRING:
                    writeGeoPoints(json, (LineString) geoValue);
                    break;
                case MULTILINESTRING:
                    for (final LineString lineString : (MultiLineString) geoValue) {
                        json.writeStartArray();
                        writeGeoPoints(json, lineString);
                        json.writeEndArray();
                    }
                    break;
                case POLYGON:
                    writeGeoPolygon(json, (Polygon) geoValue);
                    break;
                case MULTIPOLYGON:
                    for (final Polygon polygon : (MultiPolygon) geoValue) {
                        json.writeStartArray();
                        writeGeoPolygon(json, polygon);
                        json.writeEndArray();
                    }
                    break;
                case GEOSPATIALCOLLECTION:
                    for (final Geospatial element : (GeospatialCollection) geoValue) {
                        writeGeoValue(name, EdmPrimitiveTypeFactory.getInstance(element.getEdmPrimitiveTypeKind()),
                                element, isNullable, json);
                    }
                    break;
            }
            json.writeEndArray();
            json.writeEndObject();
        }
    }

    private void writeGeoPoint(JsonGenerator json, final Point point) throws IOException {
        json.writeNumber(point.getX());
        json.writeNumber(point.getY());
        if (point.getZ() != 0) {
            json.writeNumber(point.getZ());
        }
    }

    private void writeGeoPoints(JsonGenerator json, final ComposedGeospatial<Point> points) throws IOException {
        for (final Point point : points) {
            json.writeStartArray();
            writeGeoPoint(json, point);
            json.writeEndArray();
        }
    }

    // TODO: There could be a more strict verification that the lines describe boundaries
    //       and have the correct winding order.
    //       But arguably the better place for this is the constructor of the Polygon object.
    private void writeGeoPolygon(JsonGenerator json, final Polygon polygon) throws IOException {
        json.writeStartArray();
        writeGeoPoints(json, polygon.getExterior());
        json.writeEndArray();
        if (!polygon.getInterior().isEmpty()) {
            json.writeStartArray();
            writeGeoPoints(json, polygon.getInterior());
            json.writeEndArray();
        }
    }

    protected void writeComplexValue(final ServiceMetadata metadata,
                                     final EdmComplexType type, final List<Property> properties,
                                     final Set<List<String>> selectedPaths, final JsonGenerator json)
            throws IOException, SerializerException {

        for (final String propertyName : type.getPropertyNames()) {
            final Property property = findProperty(propertyName, properties);
            if (selectedPaths == null || ExpandSelectHelper.isSelected(selectedPaths, propertyName)) {
                writeProperty(metadata, (EdmProperty) type.getProperty(propertyName), property,
                        selectedPaths == null ? null : ExpandSelectHelper.getReducedSelectedPaths(selectedPaths, propertyName),
                        json);
            }
        }
    }

    private Property findProperty(final String propertyName, final List<Property> properties) {
        for (final Property property : properties) {
            if (propertyName.equals(property.getName())) {
                return property;
            }
        }
        return null;
    }


    public SerializerResult geospatialPrimitive(final ServiceMetadata metadata, final EdmPrimitiveType type,
                                                final Property property, final PrimitiveSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            writePrimitive(type, property,
                    options == null ? null : options.isNullable(),
                    options == null ? null : options.getMaxLength(),
                    options == null ? null : options.getPrecision(),
                    options == null ? null : options.getScale(),
                    options == null ? null : options.isUnicode(), json);

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } catch (final EdmPrimitiveTypeException e) {
            cachedException = new SerializerException("Wrong value for property!", e,
                    SerializerException.MessageKeys.WRONG_PROPERTY_VALUE,
                    property.getName(), property.getValue().toString());
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult primitive(final ServiceMetadata metadata, final EdmPrimitiveType type,
                                      final Property property, final PrimitiveSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            json.writeStartObject();
            writeContextURL(contextURL, json);
            writeMetadataETag(metadata, json);
            writeOperations(property.getOperations(), json);
            json.writeFieldName(property.getName());
            writePrimitive(type, property,
                    options == null ? null : options.isNullable(),
                    options == null ? null : options.getMaxLength(),
                    options == null ? null : options.getPrecision(),
                    options == null ? null : options.getScale(),
                    options == null ? null : options.isUnicode(), json);
            json.writeEndObject();

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } catch (final EdmPrimitiveTypeException e) {
            cachedException = new SerializerException("Wrong value for property!", e,
                    SerializerException.MessageKeys.WRONG_PROPERTY_VALUE,
                    property.getName(), property.getValue().toString());
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    public SerializerResult complexValue(final ServiceMetadata metadata, final EdmComplexType type,
                                         final Property property, final ComplexSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            final String name = contextURL == null ? null
                    : contextURL.getEntitySetOrSingletonOrType();
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);

            if (type.isOpenType()
                    && (type.getPropertyNames() == null
                    || type.getPropertyNames().isEmpty())) {
                writeContextURL(contextURL, json);
                writeMetadataETag(metadata, json);
                writeOpenTypeComplex(metadata, type, property, json);
            } else {
                json.writeStartObject();
                writeContextURL(contextURL, json);
                writeMetadataETag(metadata, json);

                EdmComplexType resolvedType = null;
                if (!type.getFullQualifiedName().getFullQualifiedNameAsString().
                        equals(property.getType())) {
                    if (type.getBaseType() != null
                            && type.getBaseType().getFullQualifiedName().getFullQualifiedNameAsString().
                            equals(property.getType())) {
                        resolvedType = resolveComplexType(metadata, type.getBaseType(),
                                type.getFullQualifiedName().getFullQualifiedNameAsString());
                    } else {
                        resolvedType = resolveComplexType(metadata, type, property.getType());
                    }
                } else {
                    resolvedType = resolveComplexType(metadata, type, property.getType());
                }
                if (!isODataMetadataNone && !resolvedType.equals(type) || isODataMetadataFull) {
                    json.writeStringField(Constants.JSON_TYPE, "#"
                            + resolvedType.getFullQualifiedName().getFullQualifiedNameAsString());
                }
                writeOperations(property.getOperations(), json);
                final List<Property> values
                        = property.isNull() ? Collections.<Property>emptyList() : property.asComplex().getValue();
                writeProperties(metadata, type, values, options == null ? null : options.getSelect(), json);
                if (!property.isNull() && property.isComplex()) {
                    writeNavigationProperties(metadata, type, property.asComplex(),
                            options == null ? null : options.getExpand(), null, null, name, json);
                }
                json.writeEndObject();
            }

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult complex(final ServiceMetadata metadata, final EdmComplexType type,
                                    final Property property, final ComplexSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            final String name = contextURL == null ? null
                    : contextURL.getEntitySetOrSingletonOrType();
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);

            json.writeStartObject();
            writeContextURL(contextURL, json);
            writeMetadataETag(metadata, json);
            json.writeFieldName(property.getName());

            // apply customized serializing for OpenType properties
            if (type.isOpenType()
                    && (type.getPropertyNames() == null
                    || type.getPropertyNames().isEmpty())) {
//                json.writeFieldName(property.getName());
                writeOpenTypeComplex(metadata, type, property, json);
            } else {
                json.writeStartObject();
                EdmComplexType resolvedType = null;
                if (!type.getFullQualifiedName().getFullQualifiedNameAsString().
                        equals(property.getType())) {
                    if (type.getBaseType() != null
                            && type.getBaseType().getFullQualifiedName().getFullQualifiedNameAsString().
                            equals(property.getType())) {
                        resolvedType = resolveComplexType(metadata, type.getBaseType(),
                                type.getFullQualifiedName().getFullQualifiedNameAsString());
                    } else {
                        resolvedType = resolveComplexType(metadata, type, property.getType());
                    }
                } else {
                    resolvedType = resolveComplexType(metadata, type, property.getType());
                }
                if (!isODataMetadataNone && !resolvedType.equals(type) || isODataMetadataFull) {
                    json.writeStringField(Constants.JSON_TYPE, "#"
                            + resolvedType.getFullQualifiedName().getFullQualifiedNameAsString());
                }
                writeOperations(property.getOperations(), json);

                final List<Property> values
                        = property.isNull() ? Collections.<Property>emptyList() : property.asComplex().getValue();
                writeProperties(metadata, type, values, options == null ? null : options.getSelect(), json);
                if (!property.isNull() && property.isComplex()) {
                    writeNavigationProperties(metadata, type, property.asComplex(),
                            options == null ? null : options.getExpand(), null, null, name, json);
                }
                json.writeEndObject();
            }
            json.writeEndObject();

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult primitiveCollection(final ServiceMetadata metadata, final EdmPrimitiveType type,
                                                final Property property, final PrimitiveSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            json.writeStartObject();
            writeContextURL(contextURL, json);
            writeMetadataETag(metadata, json);
            if (isODataMetadataFull) {
                json.writeStringField(Constants.JSON_TYPE, "#Collection(" + type.getFullQualifiedName().getName() + ")");
            }
            writeOperations(property.getOperations(), json);
            json.writeFieldName(Constants.VALUE);
            writePrimitiveCollection(type, property,
                    options == null ? null : options.isNullable(),
                    options == null ? null : options.getMaxLength(),
                    options == null ? null : options.getPrecision(),
                    options == null ? null : options.getScale(),
                    options == null ? null : options.isUnicode(), json);
            json.writeEndObject();

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult complexCollection(final ServiceMetadata metadata, final EdmComplexType type,
                                              final Property property, final ComplexSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            outputStream = buffer.getOutputStream();
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            json.writeStartObject();
            writeContextURL(contextURL, json);
            writeMetadataETag(metadata, json);
            if (isODataMetadataFull) {
                json.writeStringField(Constants.JSON_TYPE,
                        "#Collection(" + type.getFullQualifiedName().getFullQualifiedNameAsString() + ")");
            }
            writeOperations(property.getOperations(), json);
            json.writeFieldName(Constants.VALUE);
            writeComplexCollection(metadata, type, property, null, json);
            json.writeEndObject();

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult reference(final ServiceMetadata metadata, final EdmEntitySet edmEntitySet,
                                      final Entity entity, final ReferenceSerializerOptions options) throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;

        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            final UriHelper uriHelper = new UriHelperImpl();
            outputStream = buffer.getOutputStream();
            final JsonGenerator json = new JsonFactory().createGenerator(outputStream);

            json.writeStartObject();
            writeContextURL(contextURL, json);
//            json.writeStringField(Constants.JSON_ID, uriHelper.buildCanonicalURL(edmEntitySet, entity));
            json.writeStringField(SensorThingsEdmConstants.SELF_LINK_ANNOTATION, entity.getSelfLink().getHref());
            json.writeEndObject();

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }
    }

    @Override
    public SerializerResult referenceCollection(final ServiceMetadata metadata, final EdmEntitySet edmEntitySet,
                                                final AbstractEntityCollection entityCollection, final ReferenceCollectionSerializerOptions options)
            throws SerializerException {
        OutputStream outputStream = null;
        SerializerException cachedException = null;
        boolean pagination = false;

        try {
            final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
            CircleStreamBuffer buffer = new CircleStreamBuffer();
            final UriHelper uriHelper = new UriHelperImpl();
            outputStream = buffer.getOutputStream();
            final JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            json.writeStartObject();

            writeContextURL(contextURL, json);
            if (options != null && options.getCount() != null && options.getCount().getValue()) {
                writeInlineCount("", entityCollection.getCount(), json);
            }

            json.writeArrayFieldStart(Constants.VALUE);
            for (final Entity entity : entityCollection) {
                json.writeStartObject();
//                json.writeStringField(Constants.JSON_ID, uriHelper.buildCanonicalURL(edmEntitySet, entity));
                uriHelper.buildCanonicalURL(edmEntitySet, entity);
                json.writeStringField(SensorThingsEdmConstants.SELF_LINK_ANNOTATION, entity.getSelfLink().getHref());
                json.writeEndObject();
            }
            json.writeEndArray();

            writeNextLink(entityCollection, json, pagination);

            json.writeEndObject();

            json.close();
            outputStream.close();
            return SerializerResultImpl.with().content(buffer.getInputStream()).build();
        } catch (final IOException e) {
            cachedException
                    = new SerializerException(IO_EXCEPTION_TEXT, e, SerializerException.MessageKeys.IO_EXCEPTION);
            throw cachedException;
        } finally {
            closeCircleStreamBufferOutput(outputStream, cachedException);
        }

    }

    void writeContextURL(final ContextURL contextURL, final JsonGenerator json) throws IOException {
        if (!isODataMetadataNone && contextURL != null) {
            json.writeStringField(Constants.JSON_CONTEXT, ContextURLBuilder.create(contextURL).toASCIIString());
        }
    }

    void writeMetadataETag(final ServiceMetadata metadata, final JsonGenerator json) throws IOException {
        if (!isODataMetadataNone
                && metadata != null
                && metadata.getServiceMetadataETagSupport() != null
                && metadata.getServiceMetadataETagSupport().getMetadataETag() != null) {
            json.writeStringField(Constants.JSON_METADATA_ETAG,
                    metadata.getServiceMetadataETagSupport().getMetadataETag());
        }
    }

    void writeInlineCount(final String propertyName, final Integer count, final JsonGenerator json)
            throws IOException {
        if (count != null) {
            if (isIEEE754Compatible) {
                json.writeStringField(propertyName + replaceOdataIot(Constants.JSON_COUNT), String.valueOf(count));
            } else {
                json.writeNumberField(propertyName + replaceOdataIot(Constants.JSON_COUNT), count);
            }
        }
    }

    void writeNextLink(final AbstractEntityCollection entitySet, final JsonGenerator json, boolean pagination)
            throws IOException {
        if (entitySet.getNext() != null) {
            pagination = true;
            json.writeStringField(replaceOdataIot(Constants.JSON_NEXT_LINK), entitySet.getNext().toASCIIString());
        } else {
            pagination = false;
        }
    }

    void writeDeltaLink(final AbstractEntityCollection entitySet, final JsonGenerator json, boolean pagination)
            throws IOException {
        if (entitySet.getDeltaLink() != null && !pagination) {
            json.writeStringField(Constants.JSON_DELTA_LINK, entitySet.getDeltaLink().toASCIIString());
        }
    }

    private String replaceOdataIot(String s) {
        return s.replace("odata", "iot");
    }
}
